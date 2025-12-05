package server

import (
	"bytes"
	"context"
	"encoding/gob"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
	"github.com/mizosoft/graft"
	"github.com/mizosoft/graft/infra/api"
	"github.com/mizosoft/graft/pb"
	"go.uber.org/zap"
)

type Command struct {
	Id        string
	ServerId  string
	ClientId  string
	SmCommand any
}

type Server struct {
	sm          StateMachine
	initialized atomic.Bool
	publisher   *publisher
	srv         *http.Server
	batcher     *batcher
	G           *graft.Graft
	Mux         *http.ServeMux
	Logger      *zap.SugaredLogger
	Init        func()
}

func (s *Server) Address() string {
	return s.srv.Addr
}

func (s *Server) ListenAndServe() error {
	s.Initialize()

	go func() {
		err := s.G.ListenAndServe()
		if err != nil {
			s.Logger.Error("Graft::ListenAndServe error", zap.Error(err))
		}
	}()

	s.Logger.Info("Starting service", zap.String("address", s.Address()))
	return s.srv.ListenAndServe()
}

func (s *Server) Shutdown() error {
	err := s.srv.Shutdown(context.Background())
	s.G.Close()
	err = errors.Join(err, s.G.Persistence.Close())
	return err
}

func (s *Server) RespondOk(w http.ResponseWriter, payload any) {
	s.Respond(w, payload, http.StatusOK)
}

func (s *Server) Respond(w http.ResponseWriter, payload any, status int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(payload); err != nil {
		s.Logger.Error("Error encoding to JSON", err)
		http.Error(w, "Server error", http.StatusInternalServerError)
	}
}

func (s *Server) apply(entries []*pb.LogEntry) {
	for _, cmd := range deserializeCommands(entries) {
		response := s.sm.Apply(cmd)
		if cmd.ServerId == s.G.Id {
			s.publisher.publishOnce(cmd.Id, response)
		}
	}
}

func (s *Server) Execute(clientId string, smCommand any, w http.ResponseWriter) {
	command := Command{
		Id:        uuid.New().String(),
		ServerId:  s.G.Id,
		ClientId:  clientId,
		SmCommand: smCommand,
	}
	notify := s.publisher.subscribe(command.Id)
	if err := s.batcher.append(serializeCommand(command)); err != nil {
		s.publisher.unsubscribe(command.Id)
		if errors.Is(err, graft.ErrNotLeader) {
			s.Respond(w, api.NotLeaderResponse{
				LeaderId: s.G.LeaderId(),
			}, http.StatusForbidden)
		} else {
			s.Logger.Error("Error appending command", err)
			http.Error(w, "Internal server error", http.StatusInternalServerError)
		}
		return
	}

	result, ok := <-notify
	if !ok {
		s.Respond(w, "Command channel closed", http.StatusServiceUnavailable)
	} else {
		s.RespondOk(w, result)
	}
}

func DecodeJson[T any](r *http.Request) (T, error) {
	var req T
	decoder := json.NewDecoder(r.Body)
	err := decoder.Decode(&req)
	defer r.Body.Close()
	return req, err
}

func serializeCommand(command Command) []byte {
	buf := new(bytes.Buffer)
	encoder := gob.NewEncoder(buf)
	err := encoder.Encode(command)
	if err != nil {
		panic(err)
	}
	return buf.Bytes()
}

func deserializeCommands(entries []*pb.LogEntry) []*Command {
	commands := make([]*Command, 0, len(entries))
	for _, entry := range entries {
		var command Command
		decoder := gob.NewDecoder(bytes.NewReader(entry.Data))
		err := decoder.Decode(&command)
		if err != nil {
			panic(err)
		}
		commands = append(commands, &command)
	}
	return commands
}

func (s *Server) handlePostConfig(w http.ResponseWriter, r *http.Request) {
	req, err := DecodeJson[api.ConfigUpdateRequest](r)
	if err != nil {
		http.Error(w, "Invalid request format: "+err.Error(), http.StatusBadRequest)
		return
	}

	id := uuid.New().String()
	ch := s.publisher.subscribe(id)
	if err := s.G.ConfigUpdate(id, req.Add, req.Remove); err != nil {
		if errors.Is(err, graft.ErrNotLeader) {
			s.Respond(w, api.NotLeaderResponse{
				LeaderId: s.G.LeaderId(),
			}, http.StatusForbidden)
		} else {
			s.Logger.Error("Error updating config", err)
			http.Error(w, "Internal server error", http.StatusInternalServerError)
		}
		return
	}

	for update := range ch {
		u := update.(*pb.ConfigUpdate)
		if u.Phase == pb.ConfigUpdate_APPLIED {
			s.publisher.unsubscribe(id)
			config := make(map[string]string)
			for _, c := range u.New {
				config[c.Id] = c.Address
			}
			s.RespondOk(w, api.ConfigResponse{
				Config: config,
			})
			return
		}
	}

	s.Logger.Errorf("Config %s never applied", id)
	http.Error(w, "Internal server error", http.StatusInternalServerError)
}

func (s *Server) handleGetConfig(w http.ResponseWriter, _ *http.Request) {
	s.RespondOk(w, api.ConfigResponse{
		Config: s.G.Config(),
	})
}

func (s *Server) Initialize() {
	if !s.initialized.CompareAndSwap(false, true) {
		return
	}

	s.G.Restore = func(snapshot graft.Snapshot) error {
		return s.sm.Restore(snapshot)
	}
	s.G.Apply = func(entries []*pb.LogEntry) (bool, error) {
		s.apply(entries)
		return s.sm.ShouldSnapshot(), nil
	}
	s.G.Snapshot = func(writer io.Writer) error {
		_, err := writer.Write(s.sm.Snapshot())
		return err
	}
	s.G.ApplyConfigUpdate = func(update *pb.ConfigUpdate) error {
		s.publisher.publish(update.Id, update)
		return nil
	}

	s.Mux.HandleFunc("POST /config", s.handlePostConfig)
	s.Mux.HandleFunc("GET /config", s.handleGetConfig)

	s.batcher.init()

	s.Init()
}

func NewServer(serviceName string, address string, batchInterval time.Duration, sm StateMachine, config graft.Config) (*Server, error) {
	g, err := graft.New(config)
	if err != nil {
		return nil, err
	}

	mux := http.NewServeMux()
	server := &Server{
		sm: sm,
		publisher: &publisher{
			listeners: make(map[string]chan any),
		},
		srv: &http.Server{
			Addr:    address,
			Handler: mux,
		},
		batcher: newBatcher(g, batchInterval),
		G:       g,
		Mux:     mux,
		Logger:  config.LoggerOrNoop().With(zap.String("name", serviceName), zap.String("id", config.Id)).Sugar(),
	}
	return server, nil
}

package server

import (
	"bytes"
	"context"
	"encoding/gob"
	"encoding/json"
	"errors"
	"github.com/google/uuid"
	"github.com/mizosoft/graft"
	"github.com/mizosoft/graft/pb"
	"go.uber.org/zap"
	"net/http"
	"sync/atomic"
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

func (s *Server) Shutdown() {
	s.G.Close()
	s.G.Persistence.Close()
	err := s.srv.Shutdown(context.Background())
	if err != nil {
		s.Logger.Error("Server shutdown", zap.Error(err))
	}
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
		response := s.sm.Apply(cmd.SmCommand)
		if cmd.ServerId == s.G.Id {
			s.publisher.publishOnce(cmd.Id, response)
		}
	}
}

func (s *Server) Execute(clientId string, smCommand any, w http.ResponseWriter) {
	command := &Command{
		Id:        clientId,
		ServerId:  s.G.Id,
		ClientId:  clientId,
		SmCommand: smCommand,
	}

	notify := s.publisher.subscribe(command.Id)
	_, err := s.G.Append([][]byte{serializeCommand(command)})
	if err != nil {
		if errors.Is(err, graft.ErrNotLeader) {
			s.Respond(w, NotLeaderResponse{
				LeaderId: s.G.LeaderId(),
			}, http.StatusForbidden)
		} else {
			s.Logger.Error("Error appending command", err)
			http.Error(w, "Internal server error", http.StatusInternalServerError)
		}
		return
	}

	s.RespondOk(w, <-notify)
}

func DecodeJson[T any](r *http.Request) (T, error) {
	var req T
	decoder := json.NewDecoder(r.Body)
	err := decoder.Decode(&req)
	defer r.Body.Close()
	return req, err
}

func serializeCommand(command *Command) []byte {
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
	req, err := DecodeJson[ConfigUpdateRequest](r)
	if err != nil {
		http.Error(w, "Invalid request format: "+err.Error(), http.StatusBadRequest)
		return
	}

	id := uuid.New().String()
	ch := s.publisher.subscribe(id)
	if err := s.G.ConfigUpdate(id, req.Add, req.Remove); err != nil {
		if errors.Is(err, graft.ErrNotLeader) {
			s.Respond(w, NotLeaderResponse{
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
			s.RespondOk(w, ConfigResponse{
				Config: config,
			})
			return
		}
	}

	s.Logger.Errorf("Config %s never applied", id)
	http.Error(w, "Internal server error", http.StatusInternalServerError)
}

func (s *Server) handleGetConfig(w http.ResponseWriter, _ *http.Request) {
	s.RespondOk(w, ConfigResponse{
		Config: s.G.Config(),
	})
}

func (s *Server) Initialize() {
	if !s.initialized.CompareAndSwap(false, true) {
		return
	}

	s.G.Restore = func(snapshot graft.Snapshot) {
		s.sm.Restore(snapshot.Data())
	}
	s.G.Apply = func(entries []*pb.LogEntry) []byte {
		s.apply(entries)
		return s.sm.MaybeSnapshot()
	}
	s.G.ConfigUpdateCommitted = func(update *pb.ConfigUpdate) {
		s.publisher.publish(update.Id, update)
	}

	s.Mux.HandleFunc("POST /config", s.handlePostConfig)
	s.Mux.HandleFunc("GET /config", s.handleGetConfig)

	s.Init()
}

func NewServer(serviceName string, address string, sm StateMachine, config graft.Config) (*Server, error) {
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
		G:      g,
		Mux:    mux,
		Logger: config.LoggerOrNoop().With(zap.String("name", serviceName), zap.String("id", config.Id)).Sugar(),
	}
	return server, nil
}

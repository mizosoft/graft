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
	io.Closer

	sm        StateMachine
	started   atomic.Bool
	publisher *publisher
	srv       *http.Server
	batcher   *batcher
	G         *graft.Graft
	Mux       *http.ServeMux
	Logger    *zap.SugaredLogger
	Init      func()
}

func (s *Server) Address() string {
	return s.srv.Addr
}

func (s *Server) Start() {
	if !s.started.CompareAndSwap(false, true) {
		return
	}

	s.Mux.HandleFunc("POST /config", s.handlePostConfig)
	s.Mux.HandleFunc("GET /config", s.handleGetConfig)

	s.batcher.init()

	s.Init()

	s.G.Start(graft.Callbacks{
		Restore: func(snapshot graft.Snapshot) error {
			return s.sm.Restore(snapshot)
		},
		Apply: func(entry *pb.LogEntry) error {
			return s.apply(entry)
		},
		ShouldSnapshot: func() bool {
			return s.sm.ShouldSnapshot()
		},
		Snapshot: func(writer io.Writer) error {
			_, err := writer.Write(s.sm.Snapshot())
			return err
		},
		ApplyConfigUpdate: func(update *pb.ConfigUpdate) error {
			s.publisher.publish(update.Id, update)
			return nil
		},
	})

	go func() {
		s.Logger.Info("Starting service", zap.String("address", s.Address()))
		if err := s.srv.ListenAndServe(); err != nil {
			s.Logger.Error("ListenAndServer error", zap.Error(err))
		}
	}()
}

func (s *Server) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return errors.Join(s.srv.Shutdown(ctx), s.G.Close(), s.G.Persistence().Close())
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

func (s *Server) apply(entry *pb.LogEntry) error {
	var command Command
	decoder := gob.NewDecoder(bytes.NewReader(entry.Data))
	err := decoder.Decode(&command)
	if err != nil {
		return err
	}

	response := s.sm.Apply(command)
	if command.ServerId == s.G.Id() {
		s.publisher.publishOnce(command.Id, response)
	}
	return nil
}

func (s *Server) Execute(clientId string, smCommand any, w http.ResponseWriter) {
	command := Command{
		Id:        uuid.New().String(),
		ServerId:  s.G.Id(),
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

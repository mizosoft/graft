package service

import (
	"encoding/gob"
	"fmt"
	"net/http"
	"time"

	"github.com/google/uuid"
	"github.com/mizosoft/graft"
	"github.com/mizosoft/graft/infra/server"
	"github.com/mizosoft/graft/msgq/api"
	"go.uber.org/zap"
)

type MsgqService struct {
	q      *msgq
	server *server.Server
}

func (m *MsgqService) Id() string {
	return m.server.G.Id
}

func (m *MsgqService) Address() string {
	return m.server.Address()
}

func (m *MsgqService) Initialize() {
	m.server.Initialize()
}

func (m *MsgqService) ListenAndServe() error {
	return m.server.ListenAndServe()
}

func (m *MsgqService) Shutdown() {
	m.server.Shutdown()
}

func (m *MsgqService) handleEnqueue(w http.ResponseWriter, r *http.Request) {
	req, err := server.DecodeJson[api.EnqueueRequest](r)
	if err != nil {
		http.Error(w, "Invalid request format: "+err.Error(), http.StatusBadRequest)
		return
	}

	m.server.Execute(req.ClientId, &MsgqCommand{
		Type:  commandTypeEnqueue,
		Topic: req.Topic,
		Message: api.Message{
			Id:   fmt.Sprintf("%s/%s", req.Topic, uuid.New().String()),
			Data: req.Data,
		},
	}, w)
}

func (m *MsgqService) handleDeque(w http.ResponseWriter, r *http.Request) {
	req, err := server.DecodeJson[api.DequeRequest](r)
	if err != nil {
		http.Error(w, "Invalid request format: "+err.Error(), http.StatusBadRequest)
		return
	}

	m.server.Execute(req.ClientId, &MsgqCommand{
		Type:  commandTypeDeque,
		Topic: req.Topic,
	}, w)
}

func NewMsgqService(address string, batchInterval time.Duration, config graft.Config) (*MsgqService, error) {
	q := newMsgq(config.Logger.With(zap.String("id", config.Id)))
	srv, err := server.NewServer("MsgqService", address, batchInterval, q, config)
	if err != nil {
		return nil, err
	}
	service := &MsgqService{
		q:      q,
		server: srv,
	}
	srv.Init = func() {
		srv.Mux.HandleFunc("POST /enqueue", service.handleEnqueue)
		srv.Mux.HandleFunc("POST /deque", service.handleDeque)

		gob.Register(MsgqCommand{})
	}
	return service, nil
}

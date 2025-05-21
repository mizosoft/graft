package msgq

import (
	"encoding/gob"
	"fmt"
	"github.com/google/uuid"
	"github.com/mizosoft/graft"
	"github.com/mizosoft/graft/server"
	"net/http"
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
	req, err := server.DecodeJson[EnqueueRequest](r)
	if err != nil {
		http.Error(w, "Invalid request format: "+err.Error(), http.StatusBadRequest)
		return
	}

	m.server.Execute(req.ClientId, &MsgqCommand{
		Type:  commandTypeEnqueue,
		Topic: req.Topic,
		Message: Message{
			Id:   fmt.Sprintf("%s/%s", req.Topic, uuid.New().String()),
			Data: req.Data,
		},
	}, w)
}

func (m *MsgqService) handleDeque(w http.ResponseWriter, r *http.Request) {
	req, err := server.DecodeJson[DequeRequest](r)
	if err != nil {
		http.Error(w, "Invalid request format: "+err.Error(), http.StatusBadRequest)
		return
	}

	m.server.Execute(req.ClientId, &MsgqCommand{
		Type:  commandTypeDeque,
		Topic: req.Topic,
	}, w)
}

func NewMsgqService(address string, config graft.Config) (*MsgqService, error) {
	q := newMsgq(config.Logger)
	srv, err := server.NewServer("MsgqService", address, q, config)
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

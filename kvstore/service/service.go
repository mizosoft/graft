package service

import (
	"encoding/gob"
	"net/http"
	"time"

	"github.com/mizosoft/graft"
	"github.com/mizosoft/graft/infra/server"
	kvstore2 "github.com/mizosoft/graft/kvstore2/api"
	"go.uber.org/zap"
)

type KvService struct {
	store  *kvstore
	server *server.Server
}

func (s *KvService) Id() string {
	return s.server.G.Id
}

func (s *KvService) Address() string {
	return s.server.Address()
}

func (s *KvService) Initialize() {
	s.server.Initialize()
}

func (s *KvService) ListenAndServe() error {
	return s.server.ListenAndServe()
}

func (s *KvService) Shutdown() {
	s.server.Shutdown()
}

func (s *KvService) handleGet(w http.ResponseWriter, r *http.Request) {
	req, err := server.DecodeJson[kvstore2.GetRequest](r)
	if err != nil {
		http.Error(w, "Invalid request format: "+err.Error(), http.StatusBadRequest)
		return
	}

	if req.Linearizable {
		s.server.Execute(req.ClientId, KvCommand{
			Type: commandTypeGet,
			Key:  req.Key,
		}, w)
	} else {
		s.server.RespondOk(w, s.store.get(req.Key))
	}
}

func (s *KvService) handlePut(w http.ResponseWriter, r *http.Request) {
	req, err := server.DecodeJson[kvstore2.PutRequest](r)
	if err != nil {
		http.Error(w, "Invalid request format: "+err.Error(), http.StatusBadRequest)
		return
	}

	s.server.Execute(req.ClientId, KvCommand{
		Type:  commandTypePut,
		Key:   req.Key,
		Value: req.Value,
	}, w)
}

func (s *KvService) handlePutIfAbsent(w http.ResponseWriter, r *http.Request) {
	req, err := server.DecodeJson[kvstore2.PutRequest](r)
	if err != nil {
		http.Error(w, "Invalid request format: "+err.Error(), http.StatusBadRequest)
		return
	}

	s.server.Execute(req.ClientId, KvCommand{
		Type:  commandTypePutIfAbsent,
		Key:   req.Key,
		Value: req.Value,
	}, w)
}

func (s *KvService) handleCas(w http.ResponseWriter, r *http.Request) {
	req, err := server.DecodeJson[kvstore2.CasRequest](r)
	if err != nil {
		http.Error(w, "Invalid request format: "+err.Error(), http.StatusBadRequest)
		return
	}

	s.server.Execute(req.ClientId, KvCommand{
		Type:          commandTypeCas,
		Key:           req.Key,
		Value:         req.Value,
		ExpectedValue: req.ExpectedValue,
	}, w)
}

func (s *KvService) handleDelete(w http.ResponseWriter, r *http.Request) {
	req, err := server.DecodeJson[kvstore2.DeleteRequest](r)
	if err != nil {
		http.Error(w, "Invalid request format: "+err.Error(), http.StatusBadRequest)
		return
	}

	s.server.Execute(req.ClientId, KvCommand{
		Type: commandTypeDel,
		Key:  req.Key,
	}, w)
}

func (s *KvService) handleAppend(w http.ResponseWriter, r *http.Request) {
	req, err := server.DecodeJson[kvstore2.AppendRequest](r)
	if err != nil {
		http.Error(w, "Invalid request format: "+err.Error(), http.StatusBadRequest)
		return
	}

	s.server.Execute(req.ClientId, KvCommand{
		Type:  commandTypeAppend,
		Key:   req.Key,
		Value: req.Value,
	}, w)
}

func NewKvService(address string, batchInterval time.Duration, config graft.Config) (*KvService, error) {
	kvStore := newKvstore(config.Logger.With(zap.String("id", config.Id)))
	srv, err := server.NewServer("KvService", address, batchInterval, kvStore, config)
	if err != nil {
		return nil, err
	}
	service := &KvService{
		store:  kvStore,
		server: srv,
	}
	srv.Init = func() {
		srv.Mux.HandleFunc("POST /get", service.handleGet)
		srv.Mux.HandleFunc("POST /put", service.handlePut)
		srv.Mux.HandleFunc("POST /cas", service.handleCas)
		srv.Mux.HandleFunc("POST /delete", service.handleDelete)
		srv.Mux.HandleFunc("POST /putIfAbsent", service.handlePutIfAbsent)
		srv.Mux.HandleFunc("POST /append", service.handleAppend)

		gob.Register(KvCommand{})
	}
	return service, nil
}

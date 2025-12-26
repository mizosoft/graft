package service

import (
	"encoding/gob"
	"net/http"
	"time"

	"github.com/mizosoft/graft"
	"github.com/mizosoft/graft/dlock/api"
	"github.com/mizosoft/graft/infra"
	"github.com/mizosoft/graft/infra/server"
	"go.uber.org/zap"
)

type DlockService struct {
	lock   *dlock
	server *server.Server
	clock  infra.Clock
}

func (s *DlockService) handleLock(w http.ResponseWriter, r *http.Request) {
	req, err := server.DecodeJson[api.LockRequest](r)
	if err != nil {
		http.Error(w, "Invalid request format: "+err.Error(), http.StatusBadRequest)
		return
	}

	s.server.Execute(req.ClientId, LockCommand{
		Type:     commandTypeLock,
		Now:      s.clock.Now(),
		Resource: req.Resource,
		Ttl:      time.Duration(req.TtlMillis) * time.Millisecond,
		Fair:     req.Fair,
	}, w)
}

func (s *DlockService) handleUnlock(w http.ResponseWriter, r *http.Request) {
	req, err := server.DecodeJson[api.UnlockRequest](r)
	if err != nil {
		http.Error(w, "Invalid request format: "+err.Error(), http.StatusBadRequest)
		return
	}

	s.server.Execute(req.ClientId, LockCommand{
		Type:     commandTypeUnlock,
		Now:      s.clock.Now(),
		Resource: req.Resource,
		Token:    req.Token,
	}, w)
}

func (s *DlockService) handleRLock(w http.ResponseWriter, r *http.Request) {
	req, err := server.DecodeJson[api.LockRequest](r)
	if err != nil {
		http.Error(w, "Invalid request format: "+err.Error(), http.StatusBadRequest)
		return
	}

	s.server.Execute(req.ClientId, LockCommand{
		Type:     commandTypeRLock,
		Now:      s.clock.Now(),
		Resource: req.Resource,
		Ttl:      time.Duration(req.TtlMillis) * time.Millisecond,
		Fair:     req.Fair,
	}, w)
}

func (s *DlockService) handleRUnlock(w http.ResponseWriter, r *http.Request) {
	req, err := server.DecodeJson[api.UnlockRequest](r)
	if err != nil {
		http.Error(w, "Invalid request format: "+err.Error(), http.StatusBadRequest)
		return
	}

	s.server.Execute(req.ClientId, LockCommand{
		Type:     commandTypeRUnlock,
		Now:      s.clock.Now(),
		Resource: req.Resource,
		Token:    req.Token,
	}, w)
}

func (s *DlockService) handleRefreshLock(w http.ResponseWriter, r *http.Request) {
	req, err := server.DecodeJson[api.RefreshRequest](r)
	if err != nil {
		http.Error(w, "Invalid request format: "+err.Error(), http.StatusBadRequest)
		return
	}

	s.server.Execute(req.ClientId, LockCommand{
		Type:     commandTypeRefreshLock,
		Now:      s.clock.Now(),
		Resource: req.Resource,
		Token:    req.Token,
		Ttl:      time.Duration(req.TtlMillis) * time.Millisecond,
	}, w)
}

func (s *DlockService) handleRefreshRLock(w http.ResponseWriter, r *http.Request) {
	req, err := server.DecodeJson[api.RefreshRequest](r)
	if err != nil {
		http.Error(w, "Invalid request format: "+err.Error(), http.StatusBadRequest)
		return
	}

	s.server.Execute(req.ClientId, LockCommand{
		Type:     commandTypeRefreshRLock,
		Now:      s.clock.Now(),
		Resource: req.Resource,
		Token:    req.Token,
		Ttl:      time.Duration(req.TtlMillis) * time.Millisecond,
	}, w)
}

func NewDlockServer(address string, batchInterval time.Duration, clock infra.Clock, config graft.Config) (*server.Server, error) {
	lock := newDlock(config.Logger.With(zap.String("id", config.Id)))
	srv, err := server.NewServer("DlockService", address, batchInterval, lock, config)
	if err != nil {
		return nil, err
	}
	service := &DlockService{
		lock:   lock,
		server: srv,
		clock:  clock,
	}
	srv.Init = func() {
		srv.Mux.HandleFunc("POST /lock", service.handleLock)
		srv.Mux.HandleFunc("POST /unlock", service.handleUnlock)
		srv.Mux.HandleFunc("POST /rlock", service.handleRLock)
		srv.Mux.HandleFunc("POST /runlock", service.handleRUnlock)
		srv.Mux.HandleFunc("POST /refreshLock", service.handleRefreshLock)
		srv.Mux.HandleFunc("POST /refreshRLock", service.handleRefreshRLock)

		gob.Register(LockCommand{})

		lock.init()
	}
	return service.server, nil
}

package service

import (
	"encoding/gob"
	"net/http"
	"time"

	"github.com/mizosoft/graft"
	dlock2 "github.com/mizosoft/graft/dlock/api"
	"github.com/mizosoft/graft/infra"
	"github.com/mizosoft/graft/infra/server"
	"go.uber.org/zap"
)

type DlockService struct {
	lock   *dlock
	server *server.Server
	clock  infra.Clock
}

func (s *DlockService) Id() string {
	return s.server.G.Id
}

func (s *DlockService) Address() string {
	return s.server.Address()
}

func (s *DlockService) Initialize() {
	s.server.Initialize()
}

func (s *DlockService) ListenAndServe() error {
	return s.server.ListenAndServe()
}

func (s *DlockService) Shutdown() error {
	s.lock.close()
	return s.server.Shutdown()
}

func (s *DlockService) handleLock(w http.ResponseWriter, r *http.Request) {
	req, err := server.DecodeJson[dlock2.LockRequest](r)
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
	req, err := server.DecodeJson[dlock2.UnlockRequest](r)
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
	req, err := server.DecodeJson[dlock2.LockRequest](r)
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
	req, err := server.DecodeJson[dlock2.UnlockRequest](r)
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
	req, err := server.DecodeJson[dlock2.RefreshRequest](r)
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
	req, err := server.DecodeJson[dlock2.RefreshRequest](r)
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

func NewDlockService(address string, batchInterval time.Duration, clock infra.Clock, config graft.Config) (*DlockService, error) {
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
	return service, nil
}

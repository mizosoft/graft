package service

import (
	"encoding/gob"
	"github.com/mizosoft/graft"
	"github.com/mizosoft/graft/server"
	"net/http"
	"time"
)

type DlockService struct {
	lock   *dlock
	server *server.Server
	clock  Clock
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

func (s *DlockService) Shutdown() {
	s.lock.close()
	s.server.Shutdown()
}

func (s *DlockService) handleLock(w http.ResponseWriter, r *http.Request) {
	req, err := server.DecodeJson[LockRequest](r)
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
	req, err := server.DecodeJson[UnlockRequest](r)
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
	req, err := server.DecodeJson[LockRequest](r)
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
	req, err := server.DecodeJson[UnlockRequest](r)
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
	req, err := server.DecodeJson[RefreshRequest](r)
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
	req, err := server.DecodeJson[RefreshRequest](r)
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

type Clock interface {
	Now() time.Time
}

type systemClock struct{}

func (s *systemClock) Now() time.Time {
	return time.Now()
}

func SystemClock() Clock {
	return &systemClock{}
}

func NewDlockService(address string, clock Clock, config graft.Config) (*DlockService, error) {
	lock := newDlock(config.Logger)
	srv, err := server.NewServer("DlockService", address, lock, config)
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

package dlock

import (
	"bytes"
	"encoding/gob"
	"github.com/mizosoft/graft/server"
	"go.uber.org/zap"
	"sync"
	"sync/atomic"
	"time"
)

type CommandType int

const (
	commandTypeLock CommandType = iota
	commandTypeUnlock
	commandTypeRLock
	commandTypeRUnlock
	commandTypeRefreshLock
	commandTypeRefreshRLock
)

type LockCommand struct {
	Type     CommandType
	Resource string
	Ttl      time.Duration
	Token    uint64
	Now      time.Time
}

type Lock struct {
	Owner     string
	Token     uint64
	ROwners   map[string]time.Time
	ExpiresAt time.Time
}

type dlock struct {
	locks               map[string]*Lock
	sequencer           uint64
	redundantOperations int32 // Number of operations increasing log unnecessarily.
	logger              *zap.SugaredLogger
	gcTicker            *time.Ticker
	gcInterval          time.Duration
	closedCh            chan struct{}
	mut                 sync.Mutex
}

func (d *dlock) Apply(command *server.Command) any {
	cmd := command.SmCommand.(LockCommand)
	switch cmd.Type {
	case commandTypeLock:
		return d.lock(cmd.Now, command.ClientId, cmd.Resource, cmd.Ttl)
	case commandTypeUnlock:
		return d.unlock(cmd.Now, command.ClientId, cmd.Resource, cmd.Token)
	case commandTypeRLock:
		return d.rlock(cmd.Now, command.ClientId, cmd.Resource, cmd.Ttl)
	case commandTypeRUnlock:
		return d.runlock(cmd.Now, command.ClientId, cmd.Resource, cmd.Token)
	case commandTypeRefreshLock:
		return d.refreshLock(cmd.Now, command.ClientId, cmd.Resource, cmd.Token, cmd.Ttl)
	case commandTypeRefreshRLock:
		return d.refreshRlock(cmd.Now, command.ClientId, cmd.Resource, cmd.Token, cmd.Ttl)
	default:
		d.logger.Panicf("Unknown command type: %v", cmd.Type)
		return nil
	}
}

func (d *dlock) Restore(snapshot []byte) {
	d.mut.Lock()
	defer d.mut.Unlock()

	var mp map[string]*Lock
	decoder := gob.NewDecoder(bytes.NewReader(snapshot))
	err := decoder.Decode(&mp)
	if err != nil {
		panic(err)
	}
	d.redundantOperations = 0
	d.locks = mp
}

func (d *dlock) MaybeSnapshot() []byte {
	if atomic.LoadInt32(&d.redundantOperations) < 4096 {
		return nil
	}

	d.mut.Lock()
	defer d.mut.Unlock()

	buf := new(bytes.Buffer)
	encoder := gob.NewEncoder(buf)
	err := encoder.Encode(d.locks)
	if err != nil {
		panic(err)
	}
	return buf.Bytes()
}

func (d *dlock) nextToken() uint64 {
	return atomic.AddUint64(&d.sequencer, 1)
}

func (d *dlock) getToken() uint64 {
	return atomic.LoadUint64(&d.sequencer)
}

func (d *dlock) lock(now time.Time, clientId string, resource string, ttl time.Duration) LockResponse {
	d.logger.Info("Lock", zap.String("clientId", clientId), zap.String("resource", resource), zap.Duration("ttl", ttl))

	if clientId == "" {
		d.logger.Panic("clientId is empty")
	}

	d.mut.Lock()
	defer d.mut.Unlock()

	lock, ok := d.locks[resource]
	if ok && !lock.ExpiresAt.After(now) {
		atomic.AddInt32(&d.redundantOperations, 1)
		delete(d.locks, resource)
		ok = false
	}

	if ok {
		if lock.Owner == clientId {
			// Refresh ttl.
			lock.ExpiresAt = now.Add(ttl)
			return LockResponse{
				Success: true,
				Token:   lock.Token,
			}
		} else {
			return LockResponse{
				Success: false,
			}
		}
	} else {
		lock = &Lock{
			Owner:     clientId,
			ExpiresAt: now.Add(ttl),
			Token:     d.nextToken(),
		}
		d.locks[resource] = lock
		return LockResponse{
			Success: true,
			Token:   lock.Token,
		}
	}
}

func (d *dlock) unlock(now time.Time, clientId string, resource string, token uint64) LockResponse {
	d.logger.Info("Unlock", zap.String("clientId", clientId), zap.String("resource", resource), zap.Uint64("token", token))

	if clientId == "" {
		d.logger.Panic("clientId is empty")
	}

	d.mut.Lock()
	defer d.mut.Unlock()

	lock, ok := d.locks[resource]
	if ok && !lock.ExpiresAt.After(now) {
		atomic.AddInt32(&d.redundantOperations, 1)
		delete(d.locks, resource)
		ok = false
	}

	if ok && lock.Token == token && lock.Owner == clientId {
		atomic.AddInt32(&d.redundantOperations, 1)
		delete(d.locks, resource)
		return LockResponse{
			Success: true,
		}
	}
	return LockResponse{
		Success: false,
	}
}

func (d *dlock) rlock(now time.Time, clientId string, resource string, ttl time.Duration) LockResponse {
	d.logger.Info("RLock", zap.String("clientId", clientId), zap.String("resource", resource), zap.Duration("ttl", ttl))

	if clientId == "" {
		d.logger.Panic("clientId is empty")
	}

	d.mut.Lock()
	defer d.mut.Unlock()

	lock, ok := d.locks[resource]
	if ok && !lock.ExpiresAt.After(now) {
		atomic.AddInt32(&d.redundantOperations, 1)
		delete(d.locks, resource)
		ok = false
	}

	if ok {
		if lock.Owner != "" { // Lock is in exclusive write mode.
			return LockResponse{
				Success: false,
			}
		} else {
			expiresAt := now.Add(ttl)
			lock.ROwners[clientId] = expiresAt
			if lock.ExpiresAt.Before(expiresAt) {
				lock.ExpiresAt = expiresAt
			}
			return LockResponse{
				Success: true,
				Token:   lock.Token,
			}
		}
	} else {
		lock = &Lock{
			ExpiresAt: now.Add(ttl),
			ROwners:   make(map[string]time.Time),
			Token:     d.getToken(), // Don't increment token for readers.
		}
		lock.ROwners[clientId] = lock.ExpiresAt
		d.locks[resource] = lock
		return LockResponse{
			Success: true,
			Token:   lock.Token,
		}
	}
}

func (d *dlock) runlock(now time.Time, clientId string, resource string, token uint64) UnlockResponse {
	d.logger.Info("RUnlock", zap.String("clientId", clientId), zap.String("resource", resource), zap.Uint64("token", token))

	if clientId == "" {
		d.logger.Panic("clientId is empty")
	}

	d.mut.Lock()
	defer d.mut.Unlock()

	lock, ok := d.locks[resource]
	if ok && !lock.ExpiresAt.After(now) {
		atomic.AddInt32(&d.redundantOperations, 1)
		delete(d.locks, resource)
		ok = false
	}

	if ok && lock.Token == token && len(lock.ROwners) > 0 {
		if expiresAt, ok := lock.ROwners[clientId]; ok {
			atomic.AddInt32(&d.redundantOperations, 1)
			delete(lock.ROwners, clientId)
			if len(lock.ROwners) == 0 {
				delete(d.locks, resource)
			}
			return UnlockResponse{
				Success: expiresAt.After(now),
			}
		}
	}
	return UnlockResponse{
		Success: false,
	}
}

func (d *dlock) refreshLock(now time.Time, clientId string, resource string, token uint64, ttl time.Duration) RefreshResponse {
	d.logger.Info("RefreshLock", zap.String("clientId", clientId), zap.String("resource", resource), zap.Uint64("token", token), zap.Duration("ttl", ttl))

	if clientId == "" {
		d.logger.Panic("clientId is empty")
	}

	d.mut.Lock()
	defer d.mut.Unlock()

	lock, ok := d.locks[resource]
	if ok && !lock.ExpiresAt.After(now) {
		atomic.AddInt32(&d.redundantOperations, 1)
		delete(d.locks, resource)
		ok = false
	}

	if ok && lock.Token == token && lock.Owner == clientId {
		lock.ExpiresAt = now.Add(ttl)
		return RefreshResponse{
			Success: true,
		}
	}
	return RefreshResponse{
		Success: false,
	}
}

func (d *dlock) refreshRlock(now time.Time, clientId string, resource string, token uint64, ttl time.Duration) RefreshResponse {
	d.logger.Info("RefreshLock", zap.String("clientId", clientId), zap.String("resource", resource), zap.Uint64("token", token), zap.Duration("ttl", ttl))

	if clientId == "" {
		d.logger.Panic("clientId is empty")
	}

	d.mut.Lock()
	defer d.mut.Unlock()

	lock, ok := d.locks[resource]
	if ok && !lock.ExpiresAt.After(now) {
		atomic.AddInt32(&d.redundantOperations, 1)
		delete(d.locks, resource)
		ok = false
	}

	if ok {
		if expiresAt, ok := lock.ROwners[clientId]; ok && expiresAt.After(now) {
			newExpiresAt := now.Add(ttl)
			lock.ROwners[clientId] = newExpiresAt
			if lock.ExpiresAt.Before(newExpiresAt) {
				lock.ExpiresAt = newExpiresAt
			}
			return RefreshResponse{
				Success: true,
			}
		} else if ok {
			atomic.AddInt32(&d.redundantOperations, 1)
			delete(lock.ROwners, clientId)
		}
	}
	return RefreshResponse{
		Success: false,
	}
}

func (d *dlock) gc() {
	for {
		select {
		case <-d.gcTicker.C:
			func() {
				d.mut.Lock()
				defer d.mut.Unlock()

				now := time.Now()
				for resource, lock := range d.locks {
					if !lock.ExpiresAt.After(now) {
						atomic.AddInt32(&d.redundantOperations, 1)
						delete(d.locks, resource)
					} else if len(lock.ROwners) > 0 {
						for rowner, expiresAt := range lock.ROwners {
							if !expiresAt.After(now) {
								atomic.AddInt32(&d.redundantOperations, 1)
								delete(lock.ROwners, rowner)
							}
						}

						if len(lock.ROwners) == 0 {
							delete(d.locks, resource)
						}
					}
				}
			}()
		case <-d.closedCh:
			return
		}
	}
}

func (d *dlock) close() {
	if d.gcTicker != nil {
		d.gcTicker.Stop()
		d.gcTicker = nil
		close(d.closedCh)
	}
}

func (d *dlock) init() {
	go d.gc()
}

func newDlock(logger *zap.Logger) *dlock {
	return &dlock{
		locks:    make(map[string]*Lock),
		gcTicker: time.NewTicker(5 * time.Second),
		closedCh: make(chan struct{}),
		logger:   logger.With(zap.String("name", "dlock")).Sugar(),
	}
}

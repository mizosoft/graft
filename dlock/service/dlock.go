package service

import (
	"bytes"
	"encoding/gob"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mizosoft/graft"
	dlock2 "github.com/mizosoft/graft/dlock/api"
	"github.com/mizosoft/graft/infra/server"
	"go.uber.org/zap"
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
	Fair     bool
}

type Lock struct {
	Owner     string
	Token     uint64
	ROwners   map[string]time.Time
	ExpiresAt time.Time
}

type LockTicket struct {
	Owner     string
	ExpiresAt time.Time
	Reader    bool
}

type FairQEntry struct {
	q []LockTicket

	// Keep tickets by clientId to increase TTLs of waiting clients when they retry.
	readers map[string]*LockTicket
	writers map[string]*LockTicket
}

func (qe *FairQEntry) top(now time.Time) (LockTicket, bool) {
	for len(qe.q) > 0 {
		e := qe.q[0]
		if e.ExpiresAt.After(now) {
			return e, true
		}

		// Remove
		qe.q = qe.q[1:]
		if e.Reader {
			delete(qe.readers, e.Owner)
		} else {
			delete(qe.writers, e.Owner)
		}
	}
	return LockTicket{}, false
}

func (qe *FairQEntry) pop() {
	t := qe.q[0]
	qe.q = qe.q[1:]
	if t.Reader {
		delete(qe.readers, t.Owner)
	} else {
		delete(qe.writers, t.Owner)
	}
}

func (qe *FairQEntry) append(ticket LockTicket) {
	// If there's an existing ticket for this owner, just rewrite its TTL.
	if ticket.Reader {
		existingTicket, ok := qe.readers[ticket.Owner]
		if ok {
			if existingTicket.ExpiresAt.Before(ticket.ExpiresAt) {
				existingTicket.ExpiresAt = ticket.ExpiresAt
			}
			return
		}
	} else {
		existingTicket, ok := qe.writers[ticket.Owner]
		if ok {
			if existingTicket.ExpiresAt.Before(ticket.ExpiresAt) {
				existingTicket.ExpiresAt = ticket.ExpiresAt
			}
			return
		}
	}

	qe.q = append(qe.q, ticket)
	ticketPtr := &qe.q[len(qe.q)-1]

	if ticket.Reader {
		qe.readers[ticket.Owner] = ticketPtr
	} else {
		qe.writers[ticket.Owner] = ticketPtr
	}
}

type FairQ struct {
	entries map[string]*FairQEntry
}

func newFairQ() *FairQ {
	return &FairQ{entries: make(map[string]*FairQEntry)}
}

func (f *FairQ) top(resource string, now time.Time) (LockTicket, bool) {
	qe, ok := f.entries[resource]
	if !ok {
		return LockTicket{}, false
	}

	t, ok := qe.top(now)
	if !ok {
		delete(f.entries, resource)
		return LockTicket{}, false
	}
	return t, true
}

func (f *FairQ) pop(resource string) {
	qe, ok := f.entries[resource]
	if ok {
		qe.pop()
		if len(qe.q) == 0 {
			delete(f.entries, resource)
		}
	}
}

func (f *FairQ) append(resource string, ticket LockTicket) {
	qe, ok := f.entries[resource]
	if !ok {
		qe = &FairQEntry{
			q:       make([]LockTicket, 0),
			readers: make(map[string]*LockTicket),
			writers: make(map[string]*LockTicket),
		}
		f.entries[resource] = qe
	}
	qe.append(ticket)
}

type dlock struct {
	locks               map[string]*Lock
	fairQ               *FairQ
	sequencer           uint64
	redundantOperations int32 // Number of operations increasing log unnecessarily.
	logger              *zap.SugaredLogger
	gcTicker            *time.Ticker
	gcInterval          time.Duration
	closedCh            chan struct{}
	mut                 sync.Mutex
}

func (d *dlock) Apply(command server.Command) any {
	cmd := command.SmCommand.(LockCommand)
	switch cmd.Type {
	case commandTypeLock:
		return d.lock(cmd.Now, command.ClientId, cmd.Resource, cmd.Ttl, cmd.Fair)
	case commandTypeUnlock:
		return d.unlock(cmd.Now, command.ClientId, cmd.Resource, cmd.Token)
	case commandTypeRLock:
		return d.rlock(cmd.Now, command.ClientId, cmd.Resource, cmd.Ttl, cmd.Fair)
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

func (d *dlock) Restore(snapshot graft.Snapshot) error {
	d.mut.Lock()
	defer d.mut.Unlock()

	var mp map[string]*Lock
	decoder := gob.NewDecoder(snapshot.Reader())
	err := decoder.Decode(&mp)
	if err != nil {
		return err
	}
	d.redundantOperations = 0
	d.locks = mp
	return nil
}

func (d *dlock) ShouldSnapshot() bool {
	return atomic.LoadInt32(&d.redundantOperations) >= 4096
}

func (d *dlock) Snapshot() []byte {
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

func (d *dlock) advanceFairQ(resource string, now time.Time) {
	var lock *Lock
	for {
		ticket, ok := d.fairQ.top(resource, now)
		if !ok {
			return
		}

		if ticket.Reader { // Lock is reserved in read mode. We can give access to as many contiguous readers as we find.
			if lock == nil {
				lock = &Lock{
					ROwners:   make(map[string]time.Time),
					ExpiresAt: ticket.ExpiresAt,
					Token:     d.getToken(),
				}
				d.locks[resource] = lock
			}
			lock.ROwners[ticket.Owner] = ticket.ExpiresAt
			d.fairQ.pop(resource)
		} else if lock == nil { // Lock is reserved in write mode. We should have no existing lock.
			d.locks[resource] = &Lock{
				Owner:     ticket.Owner,
				ExpiresAt: ticket.ExpiresAt,
				Token:     d.nextToken(),
			}
			d.fairQ.pop(resource)
			return
		} else { // Lock is reserved in write mode but there are existing readers. Don't give access.
			return
		}
	}
}

func (d *dlock) currentLock(now time.Time, resource string) (*Lock, bool) {
	lock, ok := d.locks[resource]
	if ok && !lock.ExpiresAt.After(now) {
		d.deleteLock(now, resource)
		lock, ok = d.locks[resource] // We do not need to re-check for expiry as the lock is never given an expired ticket.
	}
	return lock, ok
}

func (d *dlock) deleteLock(now time.Time, resource string) {
	atomic.AddInt32(&d.redundantOperations, 1)
	delete(d.locks, resource)
	d.advanceFairQ(resource, now)
}

func (d *dlock) lock(now time.Time, clientId string, resource string, ttl time.Duration, fair bool) dlock2.LockResponse {
	d.logger.Info("Lock", zap.String("clientId", clientId), zap.String("resource", resource), zap.Duration("ttl", ttl))

	if clientId == "" {
		d.logger.Panic("clientId is empty")
	}

	d.mut.Lock()
	defer d.mut.Unlock()

	lock, ok := d.currentLock(now, resource)

	if ok {
		if lock.Owner == clientId {
			// Refresh ttl.
			lock.ExpiresAt = now.Add(ttl)
			return dlock2.LockResponse{
				Success: true,
				Token:   lock.Token,
			}
		} else {
			if fair {
				d.fairQ.append(resource, LockTicket{
					Owner:     clientId,
					ExpiresAt: now.Add(ttl),
					Reader:    false,
				})
			}
			return dlock2.LockResponse{
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
		return dlock2.LockResponse{
			Success: true,
			Token:   lock.Token,
		}
	}
}

func (d *dlock) unlock(now time.Time, clientId string, resource string, token uint64) dlock2.LockResponse {
	d.logger.Info("Unlock", zap.String("clientId", clientId), zap.String("resource", resource), zap.Uint64("token", token))

	if clientId == "" {
		d.logger.Panic("clientId is empty")
	}

	d.mut.Lock()
	defer d.mut.Unlock()

	lock, ok := d.currentLock(now, resource)

	if ok && lock.Token == token && lock.Owner == clientId {
		d.deleteLock(now, resource)
		return dlock2.LockResponse{
			Success: true,
		}
	}

	return dlock2.LockResponse{
		Success: false,
	}
}

func (d *dlock) rlock(now time.Time, clientId string, resource string, ttl time.Duration, fair bool) dlock2.LockResponse {
	d.logger.Info("RLock", zap.String("clientId", clientId), zap.String("resource", resource), zap.Duration("ttl", ttl))

	if clientId == "" {
		d.logger.Panic("clientId is empty")
	}

	d.mut.Lock()
	defer d.mut.Unlock()

	lock, ok := d.currentLock(now, resource)

	if ok {
		if lock.Owner != "" { // Lock is in exclusive write mode.
			if fair {
				d.fairQ.append(resource, LockTicket{
					Owner:     clientId,
					ExpiresAt: now.Add(ttl),
					Reader:    true,
				})
			}
			return dlock2.LockResponse{
				Success: false,
			}
		} else {
			// If we're fair & we have queued waiters (whose earliest can only be a writer because otherwise we would have
			// given it access when advancing the fair queue), don't give this new reader access.
			if fair {
				_, ok := lock.ROwners[clientId]
				if !ok { // Reader is new.
					if t, ok := d.fairQ.top(resource, now); ok {
						if t.Reader {
							d.logger.Panic("Head of fairQ is a reader while the lock is available for readers")
						}
						return dlock2.LockResponse{
							Success: false,
						}
					}
				}
			}

			expiresAt := now.Add(ttl)
			lock.ROwners[clientId] = expiresAt
			if lock.ExpiresAt.Before(expiresAt) {
				lock.ExpiresAt = expiresAt
			}
			return dlock2.LockResponse{
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
		return dlock2.LockResponse{
			Success: true,
			Token:   lock.Token,
		}
	}
}

func (d *dlock) runlock(now time.Time, clientId string, resource string, token uint64) dlock2.UnlockResponse {
	d.logger.Info("RUnlock", zap.String("clientId", clientId), zap.String("resource", resource), zap.Uint64("token", token))

	if clientId == "" {
		d.logger.Panic("clientId is empty")
	}

	d.mut.Lock()
	defer d.mut.Unlock()

	lock, ok := d.currentLock(now, resource)

	if ok && lock.Token == token && len(lock.ROwners) > 0 {
		if expiresAt, ok := lock.ROwners[clientId]; ok {
			atomic.AddInt32(&d.redundantOperations, 1)
			delete(lock.ROwners, clientId)
			if len(lock.ROwners) == 0 {
				d.deleteLock(now, resource)
			}
			return dlock2.UnlockResponse{
				Success: expiresAt.After(now),
			}
		}
	}
	return dlock2.UnlockResponse{
		Success: false,
	}
}

func (d *dlock) refreshLock(now time.Time, clientId string, resource string, token uint64, ttl time.Duration) dlock2.RefreshResponse {
	d.logger.Info("RefreshLock", zap.String("clientId", clientId), zap.String("resource", resource), zap.Uint64("token", token), zap.Duration("ttl", ttl))

	if clientId == "" {
		d.logger.Panic("clientId is empty")
	}

	d.mut.Lock()
	defer d.mut.Unlock()

	lock, ok := d.currentLock(now, resource)

	if ok && lock.Token == token && lock.Owner == clientId {
		lock.ExpiresAt = now.Add(ttl)
		return dlock2.RefreshResponse{
			Success: true,
		}
	}

	return dlock2.RefreshResponse{
		Success: false,
	}
}

func (d *dlock) refreshRlock(now time.Time, clientId string, resource string, token uint64, ttl time.Duration) dlock2.RefreshResponse {
	d.logger.Info("RefreshLock", zap.String("clientId", clientId), zap.String("resource", resource), zap.Uint64("token", token), zap.Duration("ttl", ttl))

	if clientId == "" {
		d.logger.Panic("clientId is empty")
	}

	d.mut.Lock()
	defer d.mut.Unlock()

	lock, ok := d.currentLock(now, resource)

	if ok {
		if expiresAt, ok := lock.ROwners[clientId]; ok && expiresAt.After(now) {
			newExpiresAt := now.Add(ttl)
			lock.ROwners[clientId] = newExpiresAt
			if lock.ExpiresAt.Before(newExpiresAt) {
				lock.ExpiresAt = newExpiresAt
			}
			return dlock2.RefreshResponse{
				Success: true,
			}
		} else if ok {
			atomic.AddInt32(&d.redundantOperations, 1)
			delete(lock.ROwners, clientId)
		}
	}
	return dlock2.RefreshResponse{
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

				// TODO this can be optimized by selecting a random % of keys and decide to do a full scan if too many
				//      keys are expired.
				for resource, lock := range d.locks {
					if !lock.ExpiresAt.After(now) {
						d.deleteLock(now, resource)
					} else if len(lock.ROwners) > 0 {
						for rowner, expiresAt := range lock.ROwners {
							if !expiresAt.After(now) {
								atomic.AddInt32(&d.redundantOperations, 1)
								delete(lock.ROwners, rowner)
							}
						}

						if len(lock.ROwners) == 0 {
							d.deleteLock(now, resource)
						} else {
							d.fairQ.top(resource, now) // Clean expiring entries.
						}
					} else {
						d.fairQ.top(resource, now) // Clean expiring entries.
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
		close(d.closedCh)
	}
}

func (d *dlock) init() {
	go d.gc()
}

func newDlock(logger *zap.Logger) *dlock {
	return &dlock{
		locks:    make(map[string]*Lock),
		fairQ:    newFairQ(),
		gcTicker: time.NewTicker(5 * time.Second),
		closedCh: make(chan struct{}),
		logger:   logger.With(zap.String("name", "dlock")).Sugar(),
	}
}

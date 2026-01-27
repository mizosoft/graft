package service

import (
	"bytes"
	"encoding/gob"
	"sync"
	"sync/atomic"

	"github.com/mizosoft/graft"
	"github.com/mizosoft/graft/infra/server"
	"github.com/mizosoft/graft/kvstore/api"
	"go.uber.org/zap"
)

type CommandType int

const (
	commandTypePut CommandType = iota
	commandTypePutIfAbsent
	commandTypeDel
	commandTypeGet
	commandTypeCas
	commandTypeAppend
)

type KvCommand struct {
	Type          CommandType
	Key           string
	Value         string
	ExpectedValue string
}

type kvstore struct {
	data                map[string]string
	logger              *zap.SugaredLogger
	mut                 sync.RWMutex
	redundantOperations int32 // Number of operations increasing log unnecessarily.
}

func (s *kvstore) Apply(command server.Command) any {
	cmd := command.SmCommand.(KvCommand)
	switch cmd.Type {
	case commandTypePut:
		return s.put(cmd.Key, cmd.Value)
	case commandTypePutIfAbsent:
		return s.putIfAbsent(cmd.Key, cmd.Value)
	case commandTypeDel:
		return s.del(cmd.Key)
	case commandTypeGet:
		return s.get(cmd.Key)
	case commandTypeCas:
		return s.cas(cmd.Key, cmd.ExpectedValue, cmd.Value)
	case commandTypeAppend:
		return s.append(cmd.Key, cmd.Value)
	default:
		s.logger.Panicf("Unknown command type: %v", cmd.Type)
		return nil
	}
}

func (s *kvstore) Restore(snapshot graft.Snapshot) error {
	s.mut.Lock()
	defer s.mut.Unlock()

	var mp map[string]string
	decoder := gob.NewDecoder(snapshot.Reader())
	err := decoder.Decode(&mp)
	if err != nil {
		return err
	}
	s.redundantOperations = 0
	s.data = mp
	return nil
}

func (s *kvstore) ShouldSnapshot() bool {
	return atomic.LoadInt32(&s.redundantOperations) >= 4096
}

func (s *kvstore) Snapshot() []byte {
	s.mut.RLock()
	defer s.mut.RUnlock()

	buf := new(bytes.Buffer)
	encoder := gob.NewEncoder(buf)
	err := encoder.Encode(s.data)
	if err != nil {
		panic(err)
	}
	atomic.StoreInt32(&s.redundantOperations, 0)
	return buf.Bytes()
}

func (s *kvstore) get(key string) *api.GetResponse {
	s.logger.Info("Get", zap.String("key", key))

	s.mut.RLock()
	defer s.mut.RUnlock()

	atomic.AddInt32(&s.redundantOperations, 1)
	value, ok := s.data[key]
	return &api.GetResponse{
		Exists: ok,
		Value:  value,
	}
}

func (s *kvstore) put(key string, value string) *api.PutResponse {
	s.logger.Info("Put", zap.String("key", key), zap.String("value", value))

	s.mut.Lock()
	defer s.mut.Unlock()

	prevValue, ok := s.data[key]
	s.data[key] = value
	if ok {
		atomic.AddInt32(&s.redundantOperations, 1)
	}
	return &api.PutResponse{
		Exists:        ok,
		PreviousValue: prevValue,
	}
}

func (s *kvstore) putIfAbsent(key string, value string) *api.PutIfAbsentResponse {
	s.logger.Info("PutIfAbsent", zap.String("key", key), zap.String("value", value))

	s.mut.Lock()
	defer s.mut.Unlock()

	_, ok := s.data[key]
	if ok {
		atomic.AddInt32(&s.redundantOperations, 1)
	} else {
		s.data[key] = value
	}
	return &api.PutIfAbsentResponse{
		Success: !ok,
	}
}

func (s *kvstore) del(key string) *api.DeleteResponse {
	s.logger.Info("Del", zap.String("key", key))

	s.mut.Lock()
	defer s.mut.Unlock()

	atomic.AddInt32(&s.redundantOperations, 1)
	value, ok := s.data[key]
	if ok {
		delete(s.data, key)
	}
	s.logger.Info("Delddfdf", zap.String("key", key))
	return &api.DeleteResponse{
		Exists: ok,
		Value:  value,
	}
}

func (s *kvstore) cas(key string, expectedValue string, value string) *api.CasResponse {
	s.logger.Info("Cas", zap.String("key", key), zap.String("expectedValue", expectedValue), zap.String("value", value))

	s.mut.Lock()
	defer s.mut.Unlock()

	atomic.AddInt32(&s.redundantOperations, 1)
	currValue, ok := s.data[key]
	success := false
	if (ok && currValue == expectedValue) || (!ok && expectedValue == "") {
		s.data[key] = value
		currValue = value
		ok = true
		success = true
	}
	return &api.CasResponse{
		Success: success,
		Exists:  ok,
		Value:   currValue,
	}
}

func (s *kvstore) append(key string, value string) *api.AppendResponse {
	s.logger.Info("Append", zap.String("key", key), zap.String("value", value))

	s.mut.Lock()
	defer s.mut.Unlock()

	prevValue, ok := s.data[key]
	length := len(prevValue) + len(value)
	if ok {
		s.data[key] += value
	} else {
		s.data[key] = value
	}
	return &api.AppendResponse{
		Length: length,
	}
}

func newKvStore(logger *zap.Logger) *kvstore {
	return &kvstore{
		data:   make(map[string]string),
		logger: logger.With(zap.String("name", "kvstore")).Sugar(),
	}
}

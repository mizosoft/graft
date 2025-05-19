package kvstore

import (
	"bytes"
	"encoding/gob"
	"go.uber.org/zap"
	"sync"
	"sync/atomic"
)

type CommandType = int

const (
	commandTypePut CommandType = iota
	commandTypePutIfAbsent
	commandTypeDel
	commandTypeGet
	commandTypeCas
	commandTypeAppend
)

type Command struct {
	Id            string
	ServerId      string
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

func (s *kvstore) Apply(command any) any {
	cmd := command.(Command)
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

func (s *kvstore) Restore(snapshot []byte) {
	s.mut.Lock()
	defer s.mut.Unlock()

	var mp map[string]string
	decoder := gob.NewDecoder(bytes.NewReader(snapshot))
	err := decoder.Decode(&mp)
	if err != nil {
		panic(err)
	}
	s.data = mp
}

func (s *kvstore) MaybeSnapshot() []byte {
	if atomic.LoadInt32(&s.redundantOperations) < 4096 {
		return nil
	}

	s.mut.Lock()
	defer s.mut.Unlock()

	buf := new(bytes.Buffer)
	encoder := gob.NewEncoder(buf)
	err := encoder.Encode(s.data)
	if err != nil {
		panic(err)
	}
	return buf.Bytes()
}

func (s *kvstore) get(key string) *GetResponse {
	s.logger.Info("Get", zap.String("key", key))

	s.mut.RLock()
	defer s.mut.RUnlock()

	atomic.AddInt32(&s.redundantOperations, 1)
	value, ok := s.data[key]
	return &GetResponse{
		Exists: ok,
		Value:  value,
	}
}

func (s *kvstore) put(key string, value string) *PutResponse {
	s.logger.Info("Put", zap.String("key", key), zap.String("value", value))

	s.mut.Lock()
	defer s.mut.Unlock()

	prevValue, ok := s.data[key]
	s.data[key] = value
	if ok {
		atomic.AddInt32(&s.redundantOperations, 1)
	}
	return &PutResponse{
		Exists:        ok,
		PreviousValue: prevValue,
	}
}

func (s *kvstore) putIfAbsent(key string, value string) *PutIfAbsentResponse {
	s.logger.Info("PutIfAbsent", zap.String("key", key), zap.String("value", value))

	s.mut.Lock()
	defer s.mut.Unlock()

	_, ok := s.data[key]
	if ok {
		atomic.AddInt32(&s.redundantOperations, 1)
	} else {
		s.data[key] = value
	}
	return &PutIfAbsentResponse{
		Success: !ok,
	}
}

func (s *kvstore) del(key string) *DeleteResponse {
	s.logger.Info("Del", zap.String("key", key))

	s.mut.Lock()
	defer s.mut.Unlock()

	atomic.AddInt32(&s.redundantOperations, 1)
	value, ok := s.data[key]
	if ok {
		delete(s.data, key)
	}
	s.logger.Info("Delddfdf", zap.String("key", key))
	return &DeleteResponse{
		Exists: ok,
		Value:  value,
	}
}

func (s *kvstore) cas(key string, expectedValue string, value string) *CasResponse {
	s.logger.Info("Cas", zap.String("key", key), zap.String("expectedValue", expectedValue), zap.String("value", value))

	s.mut.Lock()
	defer s.mut.Unlock()

	atomic.AddInt32(&s.redundantOperations, 1)
	currValue, ok := s.data[key]
	if ok && currValue == expectedValue {
		s.data[key] = value
		currValue = value
	}
	return &CasResponse{
		Exists: ok,
		Value:  currValue,
	}
}

func (s *kvstore) append(key string, value string) *AppendResponse {
	s.logger.Info("Append", zap.String("key", key), zap.String("value", value))

	s.mut.Lock()
	defer s.mut.Unlock()

	_, ok := s.data[key]
	if ok {
		s.data[key] += value
	} else {
		s.data[key] = value
	}
	return &AppendResponse{
		Length: len(s.data),
	}
}

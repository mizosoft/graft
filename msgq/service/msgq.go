package service

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"github.com/mizosoft/graft/infra/server"
	msgq2 "github.com/mizosoft/graft/msgq/api"
	"go.uber.org/zap"
	"strings"

	"sync"
	"sync/atomic"
)

type CommandType = int

const (
	commandTypeEnqueue CommandType = iota
	commandTypeDeque
)

type MsgqCommand struct {
	Type    CommandType
	Topic   string
	Message msgq2.Message
}

type msgq struct {
	queues              map[string][]msgq2.Message
	logger              *zap.SugaredLogger
	redundantOperations int32
	mut                 sync.Mutex
}

func (m *msgq) Apply(command *server.Command) any {
	cmd := command.SmCommand.(MsgqCommand)
	switch cmd.Type {
	case commandTypeEnqueue:
		return m.enqueue(cmd.Topic, cmd.Message)
	case commandTypeDeque:
		return m.deque(cmd.Topic)
	default:
		m.logger.Panicf("Unknown command type: %v", cmd.Type)
		return nil
	}
}

func (m *msgq) Restore(snapshot []byte) {
	m.mut.Lock()
	defer m.mut.Unlock()

	var queues map[string][]msgq2.Message
	decoder := gob.NewDecoder(bytes.NewReader(snapshot))
	err := decoder.Decode(&queues)
	if err != nil {
		panic(err)
	}
	m.redundantOperations = 0
	m.queues = queues
}

func (m *msgq) MaybeSnapshot() []byte {
	if atomic.LoadInt32(&m.redundantOperations) < 4096 {
		return nil
	}

	m.mut.Lock()
	defer m.mut.Unlock()

	buf := new(bytes.Buffer)
	encoder := gob.NewEncoder(buf)
	err := encoder.Encode(m.queues)
	if err != nil {
		panic(err)
	}
	return buf.Bytes()
}

func (m *msgq) enqueue(topic string, msg msgq2.Message) msgq2.EnqueueResponse {
	m.logger.Info("Enqueue", zap.String("topic", topic), zap.Any("message", msg), zap.String("q", strings.Trim(strings.Join(strings.Fields(fmt.Sprint(m.queues[topic])), ","), "[]")))

	m.mut.Lock()
	defer m.mut.Unlock()

	q, ok := m.queues[topic]
	if !ok {
		q = make([]msgq2.Message, 0)
		m.queues[topic] = q
	}
	m.queues[topic] = append(q, msg)
	return msgq2.EnqueueResponse{
		Index: len(q) - 1,
		Id:    msg.Id,
	}
}

func (m *msgq) deque(topic string) msgq2.DequeResponse {
	m.logger.Info("Deque", zap.String("topic", topic))

	m.mut.Lock()
	defer m.mut.Unlock()

	q, ok := m.queues[topic]
	if !ok || len(q) == 0 {
		return msgq2.DequeResponse{
			Success: false,
		}
	}

	atomic.AddInt32(&m.redundantOperations, 1)

	msg := q[0]
	if len(q) == 1 {
		delete(m.queues, topic)
	} else {
		q[0] = msgq2.Message{}
		m.queues[topic] = q[1:]
	}
	return msgq2.DequeResponse{
		Success: true,
		Message: msgq2.Message{
			Id:   msg.Id,
			Data: msg.Data,
		},
	}
}

func newMsgq(logger *zap.Logger) *msgq {
	return &msgq{
		queues: make(map[string][]msgq2.Message),
		logger: logger.With(zap.String("name", "msgq")).Sugar(),
	}
}

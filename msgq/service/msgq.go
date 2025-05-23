package service

import (
	"bytes"
	"encoding/gob"
	"github.com/mizosoft/graft/server"
	"go.uber.org/zap"

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
	Message Message
}

type msgq struct {
	queues              map[string][]Message
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

	var queues map[string][]Message
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

func (m *msgq) enqueue(topic string, msg Message) EnqueueResponse {
	m.logger.Info("Enqueue", zap.String("topic", topic), zap.Any("message", msg))

	m.mut.Lock()
	defer m.mut.Unlock()

	q, ok := m.queues[topic]
	if !ok {
		q = make([]Message, 0)
		m.queues[topic] = q
	}
	m.queues[topic] = append(q, msg)
	return EnqueueResponse{
		Index: len(q) - 1,
		Id:    msg.Id,
	}
}

func (m *msgq) deque(topic string) DequeResponse {
	m.logger.Info("Deque", zap.String("topic", topic))

	m.mut.Lock()
	defer m.mut.Unlock()

	q, ok := m.queues[topic]
	if !ok || len(q) == 0 {
		return DequeResponse{
			Success: false,
		}
	}

	atomic.AddInt32(&m.redundantOperations, 1)

	msg := q[0]
	q[0] = Message{}
	m.queues[topic] = q[1:]
	return DequeResponse{
		Success: true,
		Message: Message{
			Id:   msg.Id,
			Data: msg.Data,
		},
	}
}

func newMsgq(logger *zap.Logger) *msgq {
	return &msgq{
		queues: make(map[string][]Message),
		logger: logger.With(zap.String("name", "msgq")).Sugar(),
	}
}

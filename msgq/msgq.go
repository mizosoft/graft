package main

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
	"github.com/mizosoft/graft/raftpb"
	"log"
	"net/http"
	"reflect"
	"sync"

	"github.com/mizosoft/graft"
)

type CommandType = int

const (
	CommandTypeEnqueue CommandType = iota
	CommandTypeDeque
	CommandTypeAck
)

type Command struct {
	Type     CommandType
	ServerId string
	Id       string
	Topic    string
	Message  Message
	AutoAck  bool
}

type publisher struct {
	listeners map[string]chan any
	mut       sync.Mutex
}

func (l *publisher) listen(id string) chan any {
	l.mut.Lock()
	defer l.mut.Unlock()

	listenChan := make(chan any)
	l.listeners[id] = listenChan
	return listenChan
}

func (l *publisher) publish(id string, value any) {
	l.mut.Lock()
	defer l.mut.Unlock()

	listenChan, ok := l.listeners[id]
	if !ok {
		return
	}
	delete(l.listeners, id)
	listenChan <- value
}

type Message struct {
	Id   string
	Data string
}

type msgq struct {
	g           *graft.Graft
	queues      map[string]*Queue
	publisher   *publisher
	initialized bool
	mut         sync.Mutex
	mux         *http.ServeMux
}

type Queue struct {
	Data     []Message
	Inflight map[string]Message
}

type EnqueueRequest struct {
	Topic string `json:"topic"`
	Data  string `json:"data"`
}

type EnqueueResponse struct {
	Index int    `json:"index"`
	Id    string `json:"id"`
}

type DequeRequest struct {
	Topic   string `json:"topic"`
	AutoAck bool   `json:"autoAck"`
}

type DequeResponse struct {
	Success bool   `json:"success"`
	Topic   string `json:"topic"`
	Id      string `json:"id"`
	Data    string `json:"data"`
}

type AckRequest struct {
	Topic string `json:"topic"`
	Id    string `json:"id"`
}

type AckResponse struct {
	Success bool `json:"success"`
}

func decodeJson[T any](r *http.Request) (T, error) {
	var req T
	decoder := json.NewDecoder(r.Body)
	err := decoder.Decode(&req)
	defer r.Body.Close()
	return req, err
}

func (m *msgq) log(format string, args ...interface{}) {
	log.Printf("msgq(%s): %s\n", m.g.Id, fmt.Sprintf(format, args...))
}

func (m *msgq) respondJson(w http.ResponseWriter, payload interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(payload); err != nil {
		m.log("Error encoding to JSON: %v", err)
		http.Error(w, "Server error", http.StatusInternalServerError)
	}
}

func (m *msgq) handleEnqueue(w http.ResponseWriter, r *http.Request) {
	req, err := decodeJson[EnqueueRequest](r)
	if err != nil {
		http.Error(w, "Invalid request format: "+err.Error(), http.StatusBadRequest)
		return
	}

	executeCommand[EnqueueResponse](m, &Command{
		Id:       uuid.New().String(),
		ServerId: m.g.Id,
		Type:     CommandTypeEnqueue,
		Topic:    req.Topic,
		Message: Message{
			Id:   fmt.Sprintf("%s/%s", req.Topic, uuid.New().String()),
			Data: req.Data,
		},
	}, w)
}

func (m *msgq) handleDeque(w http.ResponseWriter, r *http.Request) {
	req, err := decodeJson[DequeRequest](r)
	if err != nil {
		http.Error(w, "Invalid request format: "+err.Error(), http.StatusBadRequest)
		return
	}

	executeCommand[DequeResponse](m, &Command{
		Id:       uuid.New().String(),
		ServerId: m.g.Id,
		Type:     CommandTypeDeque,
		Topic:    req.Topic,
		AutoAck:  req.AutoAck,
	}, w)
}

func (m *msgq) handleAck(w http.ResponseWriter, r *http.Request) {
	req, err := decodeJson[AckRequest](r)
	if err != nil {
		http.Error(w, "Invalid request format: "+err.Error(), http.StatusBadRequest)
		return
	}

	executeCommand[AckResponse](m, &Command{
		Id:       uuid.New().String(),
		ServerId: m.g.Id,
		Type:     CommandTypeAck,
		Topic:    req.Topic,
		Message: Message{
			Id: req.Id,
		},
	}, w)
}

func executeCommand[T any](m *msgq, command *Command, w http.ResponseWriter) {
	notify := m.publisher.listen(command.Id)
	_, err := m.g.Append([][]byte{serializeCommand(command)})
	if err != nil {
		m.log("Error appending command: %v", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	result := <-notify
	if _, ok := result.(*T); !ok {
		var temp *T
		m.log("Couldn't cast (%v) response to %v", reflect.TypeOf(result).String(), reflect.TypeOf(temp).String())
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	m.respondJson(w, result)
}

func serializeCommand(command *Command) []byte {
	buf := new(bytes.Buffer)
	encoder := gob.NewEncoder(buf)
	err := encoder.Encode(command)
	if err != nil {
		panic(err)
	}
	return buf.Bytes()
}

func deserializeCommands(entries []*raftpb.LogEntry) []*Command {
	commands := make([]*Command, 0, len(entries))
	for _, entry := range entries {
		var command Command
		decoder := gob.NewDecoder(bytes.NewReader(entry.Command))
		err := decoder.Decode(&command)
		if err != nil {
			panic(err)
		}
		commands = append(commands, &command)
	}
	return commands
}

func (m *msgq) apply(entries []*raftpb.LogEntry) {
	for _, cmd := range deserializeCommands(entries) {
		response := m.processCommand(cmd)
		if cmd.ServerId == m.g.Id {
			m.publisher.publish(cmd.Id, response)
		}
	}
}

func (m *msgq) processCommand(cmd *Command) any {
	switch cmd.Type {
	case CommandTypeEnqueue:
		return m.enqueue(cmd.Topic, cmd.Message)
	case CommandTypeDeque:
		return m.deque(cmd.Topic, cmd.AutoAck)
	case CommandTypeAck:
		return m.ack(cmd.Topic, cmd.Message.Id)
	default:
		log.Panicf("unknown command type: %v", cmd.Type)
		return nil
	}
}

func (m *msgq) enqueue(topic string, msg Message) *EnqueueResponse {
	m.mut.Lock()
	defer m.mut.Unlock()
	fmt.Println("Enqueuing", topic, msg)

	q, ok := m.queues[topic]
	if !ok {
		q = &Queue{
			Data:     make([]Message, 0),
			Inflight: make(map[string]Message),
		}
		m.queues[topic] = q
	}
	q.Data = append(q.Data, msg)
	fmt.Println(q.Data)
	return &EnqueueResponse{
		Index: len(q.Data) - 1,
		Id:    msg.Id,
	}
}

func (m *msgq) deque(topic string, autoAck bool) *DequeResponse {
	m.mut.Lock()
	defer m.mut.Unlock()

	q, ok := m.queues[topic]
	if !ok || len(q.Data) == 0 {
		return &DequeResponse{
			Success: false,
		}
	}

	msg := q.Data[0]
	q.Data[0] = Message{}
	q.Data = q.Data[1:]
	if !autoAck {
		q.Inflight[msg.Id] = msg
	}
	return &DequeResponse{
		Success: true,
		Id:      msg.Id,
		Data:    msg.Data,
	}
}

func (m *msgq) ack(topic string, id string) *AckResponse {
	m.mut.Lock()
	defer m.mut.Unlock()

	q, ok := m.queues[topic]
	if !ok {
		return &AckResponse{
			Success: false,
		}
	}

	_, ok = q.Inflight[id]
	delete(q.Inflight, id)
	return &AckResponse{
		Success: ok,
	}
}

func newMsgq(g *graft.Graft) *msgq {
	return &msgq{
		g:      g,
		queues: make(map[string]*Queue),
		publisher: &publisher{
			listeners: make(map[string]chan any),
		},
		mux: http.NewServeMux(),
	}
}

func (m *msgq) registerHandlers() {
	m.mux.HandleFunc("POST /enqueue", m.handleEnqueue)
	m.mux.HandleFunc("POST /deque", m.handleDeque)
	m.mux.HandleFunc("POST /ack", m.handleAck)
}

func (m *msgq) listenAndServe(address string) error {
	return http.ListenAndServe(address, m.mux)
}

func serializeQueues(queues map[string]Queue) []byte {
	buf := new(bytes.Buffer)
	encoder := gob.NewEncoder(buf)
	err := encoder.Encode(queues)
	if err != nil {
		panic(err)
	}
	return buf.Bytes()
}

func deserializeQueues(data []byte) map[string]*Queue {
	var queues map[string]*Queue
	decoder := gob.NewDecoder(bytes.NewReader(data))
	err := decoder.Decode(&queues)
	if err != nil {
		panic(err)
	}
	return queues
}

func (m *msgq) initialize() {
	m.mut.Lock()
	defer m.mut.Unlock()

	if m.initialized {
		return
	}
	m.initialized = true

	m.registerHandlers()
	m.g.Restore = func(snapshot graft.Snapshot) {
		m.queues = deserializeQueues(snapshot.Data())
	}
	m.g.Apply = func(entries []*raftpb.LogEntry) []byte {
		m.apply(entries)
		return nil
	}
}

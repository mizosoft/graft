package main

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/google/uuid"
	"go.uber.org/zap"
	"net/http"
	"reflect"
	"sync"

	"github.com/mizosoft/graft"
	"github.com/mizosoft/graft/pb"
)

type CommandType = int

const (
	commandTypeEnqueue CommandType = iota
	commandTypeDeque
	commandTypeAck
)

type Command struct {
	Type     CommandType
	ServerId string
	Id       string
	Topic    string
	Message  Message
	AutoAck  bool
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
	logger      *zap.SugaredLogger
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

type ConfigUpdateRequest struct {
	Add    map[string]string `json:"add"`
	Remove []string          `json:"remove"`
}

type ConfigUpdateResponse struct {
	Config map[string]string `json:"config"`
}

type NotLeaderResponse struct {
	LeaderId string `json:"leaderId"`
}

func decodeJson[T any](r *http.Request) (T, error) {
	var req T
	decoder := json.NewDecoder(r.Body)
	err := decoder.Decode(&req)
	defer r.Body.Close()
	return req, err
}

func (m *msgq) respondJson(w http.ResponseWriter, payload interface{}) {
	m.respondJsonWithStatus(w, payload, http.StatusOK)
}

func (m *msgq) respondJsonWithStatus(w http.ResponseWriter, payload interface{}, status int) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	if err := json.NewEncoder(w).Encode(payload); err != nil {
		m.logger.Error("Error encoding to JSON", err)
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
		Type:     commandTypeEnqueue,
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
		Type:     commandTypeDeque,
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
		Type:     commandTypeAck,
		Topic:    req.Topic,
		Message: Message{
			Id: req.Id,
		},
	}, w)
}

func (m *msgq) handleConfigUpdate(w http.ResponseWriter, r *http.Request) {
	req, err := decodeJson[ConfigUpdateRequest](r)
	if err != nil {
		http.Error(w, "Invalid request format: "+err.Error(), http.StatusBadRequest)
		return
	}

	id := uuid.New().String()
	ch := m.publisher.subscribe(id)
	if err := m.g.ConfigUpdate(id, req.Add, req.Remove); err != nil {
		if errors.Is(err, graft.ErrNotLeader) {
			m.respondJsonWithStatus(w, &NotLeaderResponse{
				LeaderId: m.g.LeaderId(),
			}, http.StatusMisdirectedRequest)
		} else {
			m.logger.Error("Error updating config", err)
			http.Error(w, "Internal server error", http.StatusInternalServerError)
		}
		return
	}

	for update := range ch {
		u := update.(*pb.ConfigUpdate)
		if u.Phase == pb.ConfigUpdate_APPLIED {
			m.publisher.unsubscribe(id)
			config := make(map[string]string)
			for _, c := range u.New {
				config[c.Id] = c.Address
			}
			m.respondJson(w, &ConfigUpdateResponse{
				Config: config,
			})
			return
		}
	}

	m.logger.Errorf("Config %s never applied", id)
	http.Error(w, "Internal server error", http.StatusInternalServerError)
}

func executeCommand[T any](m *msgq, command *Command, w http.ResponseWriter) {
	notify := m.publisher.subscribe(command.Id)
	_, err := m.g.Append([][]byte{serializeCommand(command)})
	if err != nil {
		m.logger.Error("Error appending command", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	result := <-notify
	if _, ok := result.(*T); !ok {
		var temp *T
		m.logger.Error("Couldn't cast (%v) response to %v", reflect.TypeOf(result).String(), reflect.TypeOf(temp).String())
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

func deserializeCommands(entries []*pb.LogEntry) []*Command {
	commands := make([]*Command, 0, len(entries))
	for _, entry := range entries {
		var command Command
		decoder := gob.NewDecoder(bytes.NewReader(entry.Data))
		err := decoder.Decode(&command)
		if err != nil {
			panic(err)
		}
		commands = append(commands, &command)
	}
	return commands
}

func (m *msgq) apply(entries []*pb.LogEntry) {
	for _, cmd := range deserializeCommands(entries) {
		response := m.processCommand(cmd)
		if cmd.ServerId == m.g.Id {
			m.publisher.publish(cmd.Id, response)
		}
	}
}

func (m *msgq) processCommand(cmd *Command) any {
	switch cmd.Type {
	case commandTypeEnqueue:
		return m.enqueue(cmd.Topic, cmd.Message)
	case commandTypeDeque:
		return m.deque(cmd.Topic, cmd.AutoAck)
	case commandTypeAck:
		return m.ack(cmd.Topic, cmd.Message.Id)
	default:
		m.logger.Errorf("Unknown command type: %v", cmd.Type)
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

func newMsgq(g *graft.Graft, logger *zap.Logger) *msgq {
	return &msgq{
		g:      g,
		queues: make(map[string]*Queue),
		publisher: &publisher{
			listeners: make(map[string]chan any),
		},
		mux:    http.NewServeMux(),
		logger: logger.With(zap.String("id", g.Id)).Sugar(),
	}
}

func (m *msgq) registerHandlers() {
	m.mux.HandleFunc("POST /enqueue", m.handleEnqueue)
	m.mux.HandleFunc("POST /deque", m.handleDeque)
	m.mux.HandleFunc("POST /ack", m.handleAck)
	m.mux.HandleFunc("POST /config", m.handleConfigUpdate)
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
	m.g.Apply = func(entries []*pb.LogEntry) []byte {
		m.apply(entries)
		return nil
	}
}

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
	CommandTypePut CommandType = iota
	CommandTypePutIfAbsent
	CommandTypeDel
	CommandTypeGet
	CommandTypeCas
)

type Command struct {
	Id            string
	ServerId      string
	Type          CommandType
	Key           string
	Value         string
	ExpectedValue string
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

type kvstore struct {
	data         map[string]string
	publisher    *publisher
	snapshotChan chan graft.Snapshot // Restore from snapshot channel.
	g            *graft.Graft
	initialized  bool
	mut          sync.RWMutex
	mux          *http.ServeMux

	count int
}

type GetRequest struct {
	Key          string `json:"key"`
	Linearizable bool   `json:"linearizable"`
}

type GetResponse struct {
	Exists bool   `json:"exists"`
	Value  string `json:"value"`
}

type PutRequest struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

type PutResponse struct {
	Exists        bool   `json:"exists"`
	PreviousValue string `json:"previousValue"`
}

type CasRequest struct {
	Key           string `json:"key"`
	Value         string `json:"value"`
	ExpectedValue string `json:"expectedValue"`
}

type CasResponse struct {
	Exists bool   `json:"exists"`
	Value  string `json:"currValue"`
}

type DeleteRequest struct {
	Key string `json:"key"`
}

func (s *kvstore) log(format string, args ...interface{}) {
	log.Printf("kvstore(%s): %s\n", s.g.Id, fmt.Sprintf(format, args...))
}

func (s *kvstore) respondJson(w http.ResponseWriter, payload interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(w).Encode(payload); err != nil {
		s.log("Error encoding to JSON: %v", err)
		http.Error(w, "Server error", http.StatusInternalServerError)
	}
}

func executeCommand[T any](s *kvstore, command *Command, w http.ResponseWriter) {
	notify := s.publisher.listen(command.Id)
	_, err := s.g.Append([][]byte{serializeCommand(command)})
	if err != nil {
		s.log("Error appending command: %v", err)
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	result := <-notify
	if _, ok := result.(*T); !ok {
		var temp *T
		s.log("Couldn't cast (%v) response to %v", reflect.TypeOf(result).String(), reflect.TypeOf(temp).String())
		http.Error(w, "Internal server error", http.StatusInternalServerError)
		return
	}

	s.respondJson(w, result)
}

func (s *kvstore) handleGet(w http.ResponseWriter, r *http.Request) {
	req, err := decodeJson[GetRequest](r)
	if err != nil {
		http.Error(w, "Invalid request format: "+err.Error(), http.StatusBadRequest)
		return
	}

	if req.Linearizable {
		executeCommand[GetResponse](s, &Command{
			Id:       uuid.New().String(),
			ServerId: s.g.Id,
			Type:     CommandTypeGet,
			Key:      req.Key,
		}, w)
	} else {
		value, ok := func() (string, bool) {
			s.mut.RLock()
			defer s.mut.RUnlock()

			value, ok := s.data[req.Key]
			return value, ok
		}()

		s.respondJson(w, &GetResponse{
			Value:  value,
			Exists: ok,
		})
	}
}

func (s *kvstore) handlePut(w http.ResponseWriter, r *http.Request) {
	req, err := decodeJson[PutRequest](r)
	if err != nil {
		http.Error(w, "Invalid request format: "+err.Error(), http.StatusBadRequest)
		return
	}

	executeCommand[PutResponse](s, &Command{
		Id:       uuid.New().String(),
		ServerId: s.g.Id,
		Type:     CommandTypePut,
		Key:      req.Key,
		Value:    req.Value,
	}, w)
}

func (s *kvstore) handlePutIfAbsent(w http.ResponseWriter, r *http.Request) {
	req, err := decodeJson[PutRequest](r)
	if err != nil {
		http.Error(w, "Invalid request format: "+err.Error(), http.StatusBadRequest)
		return
	}

	executeCommand[PutIfAbsentResponse](s, &Command{
		Id:       uuid.New().String(),
		ServerId: s.g.Id,
		Type:     CommandTypePutIfAbsent,
		Key:      req.Key,
		Value:    req.Value,
	}, w)
}

func (s *kvstore) handleCas(w http.ResponseWriter, r *http.Request) {
	req, err := decodeJson[CasRequest](r)
	if err != nil {
		http.Error(w, "Invalid request format: "+err.Error(), http.StatusBadRequest)
		return
	}

	executeCommand[CasResponse](s, &Command{
		Id:            uuid.New().String(),
		ServerId:      s.g.Id,
		Type:          CommandTypeCas,
		Key:           req.Key,
		Value:         req.Value,
		ExpectedValue: req.ExpectedValue,
	}, w)
}

func (s *kvstore) handleDelete(w http.ResponseWriter, r *http.Request) {
	req, err := decodeJson[DeleteRequest](r)
	if err != nil {
		http.Error(w, "Invalid request format: "+err.Error(), http.StatusBadRequest)
		return
	}

	executeCommand[DeleteResponse](s, &Command{
		Id:       uuid.New().String(),
		ServerId: s.g.Id,
		Type:     CommandTypeDel,
		Key:      req.Key,
	}, w)
}

func decodeJson[T any](r *http.Request) (T, error) {
	var req T
	decoder := json.NewDecoder(r.Body)
	err := decoder.Decode(&req)
	defer r.Body.Close()
	return req, err
}

func (s *kvstore) apply(entries []*raftpb.LogEntry) {
	for _, cmd := range deserializeCommands(entries) {
		response := s.processCommand(cmd)
		if cmd.ServerId == s.g.Id {
			s.publisher.publish(cmd.Id, response)
		}
	}
}

func (s *kvstore) processCommand(cmd *Command) any {
	switch cmd.Type {
	case CommandTypePut:
		return s.put(cmd.Key, cmd.Value)
	case CommandTypePutIfAbsent:
		return s.putIfAbsent(cmd.Key, cmd.Value)
	case CommandTypeDel:
		return s.del(cmd.Key)
	case CommandTypeGet:
		return s.get(cmd.Key)
	case CommandTypeCas:
		return s.cas(cmd.Key, cmd.ExpectedValue, cmd.Value)
	default:
		log.Panicf("unknown command type: %v", cmd.Type)
		return nil
	}
}

func (s *kvstore) get(key string) *GetResponse {
	s.log("Get(%s)", key)

	s.mut.RLock()
	defer s.mut.RUnlock()

	value, ok := s.data[key]
	return &GetResponse{
		Exists: ok,
		Value:  value,
	}
}

func (s *kvstore) put(key string, value string) *PutResponse {
	s.log("Put(%s, %s)", key, value)

	s.mut.Lock()
	defer s.mut.Unlock()

	prevValue, ok := s.data[key]
	s.data[key] = value
	return &PutResponse{
		Exists:        ok,
		PreviousValue: prevValue,
	}
}

type PutIfAbsentResponse struct {
	Exists bool `json:"exists"`
}

func (s *kvstore) putIfAbsent(key string, value string) *PutIfAbsentResponse {
	s.log("PutIfAbsent(%s, %s)", key, value)

	s.mut.Lock()
	defer s.mut.Unlock()

	_, ok := s.data[key]
	if ok {
		s.data[key] = value
	}
	return &PutIfAbsentResponse{
		Exists: ok,
	}
}

type DeleteResponse struct {
	Exists bool   `json:"exists"`
	Value  string `json:"value"`
}

func (s *kvstore) del(key string) *DeleteResponse {
	s.log("Del(%s)", key)

	s.mut.Lock()
	defer s.mut.Unlock()

	value, ok := s.data[key]
	if ok {
		delete(s.data, key)
	}
	return &DeleteResponse{
		Exists: ok,
		Value:  value,
	}
}

func (s *kvstore) cas(key string, expectedValue string, value string) *CasResponse {
	s.log("Cas(%s, %s, %s)", key, expectedValue, value)

	s.mut.Lock()
	defer s.mut.Unlock()

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

func (s *kvstore) registerHandlers() {
	s.mux.HandleFunc("POST /get", s.handleGet)
	s.mux.HandleFunc("POST /put", s.handlePut)
	s.mux.HandleFunc("POST /cas", s.handleCas)
	s.mux.HandleFunc("POST /delete", s.handleDelete)
}

func (s *kvstore) listenAndServe(address string) error {
	return http.ListenAndServe(address, s.mux)
}

func serializeData(data map[string]string) []byte {
	buf := new(bytes.Buffer)
	encoder := gob.NewEncoder(buf)
	err := encoder.Encode(data)
	if err != nil {
		panic(err)
	}
	return buf.Bytes()
}

func deserializeData(data []byte) map[string]string {
	var mp map[string]string
	decoder := gob.NewDecoder(bytes.NewReader(data))
	err := decoder.Decode(&mp)
	if err != nil {
		panic(err)
	}
	return mp
}

func newKvStore(g *graft.Graft) *kvstore {
	return &kvstore{
		g:    g,
		mux:  http.NewServeMux(),
		data: make(map[string]string),
		publisher: &publisher{
			listeners: make(map[string]chan any),
		},
	}
}

func (s *kvstore) initialize() {
	s.mut.Lock()
	defer s.mut.Unlock()

	if s.initialized {
		return
	}
	s.initialized = true

	s.registerHandlers()
	s.g.Restore = func(snapshot graft.Snapshot) {
		s.mut.Lock()
		defer s.mut.Unlock()
		s.data = deserializeData(snapshot.Data())
	}
	s.g.Apply = func(entries []*raftpb.LogEntry) []byte {
		s.apply(entries)
		s.count += len(entries)
		if s.count >= 4 {
			return serializeData(s.data)
		}
		return nil
	}
}

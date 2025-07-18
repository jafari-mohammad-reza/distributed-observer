package tracker

import (
	"encoding/json"
	"observer/conf"
	"observer/event"
	"observer/server"
	"observer/share"
	"sync"
	"time"

	"github.com/google/uuid"
)

type ITracker interface {
	Start() error
	StartTransaction(payload share.StartTransactionPayload) (string, error)
	StartSpan(payload share.StartSpanPayload) (string, error)
	EndSpan(id string) error
	EndTransaction(id string) error
	SearchTransaction() ([]*Transaction, error)
	SearchSpan() ([]*Span, error)
}

type Tracker struct {
	conf                  *conf.Config
	eventHandler          event.EventHandler
	mu                    sync.RWMutex
	active_transactions   map[string]*Transaction
	finished_transactions map[string]*Transaction
	tcpServer             server.Server
}

func NewTracker(conf *conf.Config, handler event.EventHandler) ITracker {
	tracker := Tracker{
		conf:                  conf,
		eventHandler:          handler,
		mu:                    sync.RWMutex{},
		active_transactions:   make(map[string]*Transaction, 1000),
		finished_transactions: make(map[string]*Transaction),
	}
	tracker.tcpServer = server.NewServer(conf, handler, tracker.connectionHandler)
	return &tracker
}
func (t *Tracker) Start() error {
	go func() {
		err := t.tcpServer.Start(t.conf.Tracker.Port)
		if err != nil {
			panic(err)
		}
	}()
	return nil
}
func (t *Tracker) StartTransaction(payload share.StartTransactionPayload) (string, error) {
	t.mu.RLock()
	defer t.mu.RUnlock()
	transaction := Transaction{
		ID:      uuid.NewString(),
		Name:    payload.Name,
		Spans:   make(map[string]*Span),
		Start:   time.Now(),
		Headers: payload.Headers,
	}
	t.active_transactions[transaction.ID] = &transaction
	return transaction.ID, nil
}
func (t *Tracker) StartSpan(payload share.StartSpanPayload) (string, error) {
	return "", nil
}
func (t *Tracker) EndSpan(id string) error {
	return nil
}
func (t *Tracker) EndTransaction(id string) error {
	return nil
}
func (t *Tracker) SearchTransaction() ([]*Transaction, error) {
	return nil, nil
}
func (t *Tracker) SearchSpan() ([]*Span, error) {
	return nil, nil
}
func (t *Tracker) connectionHandler(packet *share.TransferPacket) {
	command := packet.Command
	switch command {
	case share.StartTransactionCommand:
		var payload share.StartTransactionPayload
		err := json.Unmarshal(packet.Payload, &payload)
		if err != nil {
			share.RespondConn(*packet.Conn, []byte(err.Error()))
			return
		}
		id, err := t.StartTransaction(payload)
		if err != nil {
			share.RespondConn(*packet.Conn, []byte(err.Error()))
			return
		}
		share.RespondConn(*packet.Conn, []byte(id))
	case share.EndTransactionCommand:
		err := t.EndTransaction(string(packet.Payload))
		if err != nil {
			share.RespondConn(*packet.Conn, []byte(err.Error()))
			return
		}
		share.RespondConn(*packet.Conn, []byte("transaction ended"))
	case share.StartSpanCommand:
		var payload share.StartSpanPayload
		err := json.Unmarshal(packet.Payload, &payload)
		if err != nil {
			share.RespondConn(*packet.Conn, []byte(err.Error()))
			return
		}
		id, err := t.StartSpan(payload)
		if err != nil {
			share.RespondConn(*packet.Conn, []byte(err.Error()))
			return
		}
		share.RespondConn(*packet.Conn, []byte(id))
	case share.EndSpanCommand:
		err := t.EndSpan(string(packet.Payload))
		if err != nil {
			share.RespondConn(*packet.Conn, []byte(err.Error()))
			return
		}
		share.RespondConn(*packet.Conn, []byte("span ended"))
	default:
		share.RespondConn(*packet.Conn, []byte("invalid command"))
	}
}

type Transaction struct {
	ID      string            `json:"id"`
	Name    string            `json:"name"`
	Spans   map[string]*Span  `json:"spans"` // spanId to span
	Start   time.Time         `json:"start"`
	Headers map[string]string `json:"headers"`
}

type Span struct {
	ID       string          `json:"id"`
	Name     string          `json:"name"`
	ParentID string          `json:"parent_id"`
	Meta     json.RawMessage `json:"meta"`
	Start    time.Time       `json:"start"`
	End      time.Time       `json:"end"`
	Duration time.Duration   `json:"duration"`
}

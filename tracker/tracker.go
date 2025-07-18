package tracker

import (
	"encoding/json"
	"errors"
	"fmt"
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
	SearchTransaction(id string) (*Transaction, error)
	SearchSpan(id string) (*Span, error)
	Stats() *TrackerStats
}

type Tracker struct {
	conf                  *conf.Config
	eventHandler          event.EventHandler
	mu                    sync.RWMutex
	active_transactions   map[string]*Transaction
	active_spans          map[string]*Span
	finished_transactions map[string]*Transaction
	tcpServer             server.Server
}

type Transaction struct {
	ID       string            `json:"id"`
	Name     string            `json:"name"`
	Spans    map[string]*Span  `json:"spans"` // spanId to span
	Start    time.Time         `json:"start"`
	Headers  map[string]string `json:"headers"`
	End      time.Time         `json:"end"`
	Duration time.Duration     `json:"duration"`
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
type TrackerStats struct {
	ActiveTransactionsCount   int
	ActiveTransactions        []*Transaction
	FinishedTransactionsCount int
	ActiveSpansCount          int
}

func NewTracker(conf *conf.Config, handler event.EventHandler) ITracker {
	tracker := Tracker{
		conf:                  conf,
		eventHandler:          handler,
		mu:                    sync.RWMutex{},
		active_transactions:   make(map[string]*Transaction, 1000),
		finished_transactions: make(map[string]*Transaction),
		active_spans:          make(map[string]*Span),
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
func (t *Tracker) Stats() *TrackerStats {
	stat := TrackerStats{
		ActiveTransactionsCount:   len(t.active_transactions),
		FinishedTransactionsCount: len(t.finished_transactions),
		ActiveSpansCount:          len(t.active_spans),
	}
	for _, transaction := range t.active_transactions {
		stat.ActiveTransactions = append(stat.ActiveTransactions, transaction)
	}
	return &stat
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
	t.mu.RLock()
	defer t.mu.RUnlock()
	transaction, err := t.SearchTransaction(payload.ParentID)
	if err != nil {
		return "", err
	}
	span, ok := transaction.Spans[payload.ParentID]
	if !ok {
		sp := Span{
			ID:       uuid.NewString(),
			Name:     payload.Name,
			Meta:     payload.Meta,
			ParentID: payload.ParentID,
			Start:    time.Now(),
		}
		transaction.Spans[sp.ID] = &sp
		t.active_spans[sp.ID] = &sp
		return sp.ID, err
	}
	span.Meta = append(span.Meta, payload.Meta...)
	return span.ID, nil
}
func (t *Tracker) EndSpan(id string) error {
	sp, err := t.SearchSpan(id)
	if err != nil {
		return err
	}
	delete(t.active_spans, sp.ID)
	sp.End = time.Now()
	sp.Duration = sp.End.Sub(sp.Start)
	return nil
}
func (t *Tracker) EndTransaction(id string) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	tr, err := t.SearchTransaction(id)
	if err != nil {
		return err
	}
	tr.End = time.Now()
	tr.Duration = tr.End.Sub(tr.Start)
	for _, sp := range tr.Spans {
		err := t.EndSpan(sp.ID)
		if err != nil {
			fmt.Printf("failed to end span %s\n", sp.ID)
		}
	}
	t.finished_transactions[id] = tr

	delete(t.active_transactions, id)

	return nil
}
func (t *Tracker) SearchTransaction(id string) (*Transaction, error) {
	tr, ok := t.active_transactions[id]
	if !ok {
		return nil, errors.New("transaction not found")
	}
	return tr, nil
}
func (t *Tracker) SearchSpan(id string) (*Span, error) {
	span, ok := t.active_spans[id]
	if !ok {
		return nil, errors.New("span not found")
	}
	return span, nil
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

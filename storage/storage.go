package storage

import (
	"distributed-observer/conf"
	"distributed-observer/event"
	"distributed-observer/share"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"
)

type StorageManager interface {
	ConsumeMutations() error
}
type Storage interface {
	Insert(payload share.MutatePayload) error
}

type MemManager struct {
	conf         *conf.Config
	storage      Storage
	eventHandler event.EventHandler
}

func NewStorageManager(conf *conf.Config, eventHandler event.EventHandler) StorageManager {
	return &MemManager{
		conf:         conf,
		storage:      NewMemStorage(conf),
		eventHandler: eventHandler,
	}
}

type IndexStore struct {
	Key     string // hash of index + timeMin + timeMax
	TimeMin time.Time
	TimeMax time.Time
	Index   string
	Data    map[string][]string // value to document ids
}

type MemStorage struct {
	mu       sync.Mutex
	conf     *conf.Config
	Segments map[string][]*IndexStore
}

func (m *MemStorage) Insert(payload share.MutatePayload) error {
	ts, err := time.Parse(time.RFC3339, payload.Timestamp)
	if err != nil {
		return err
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	var segment *IndexStore
	for _, seg := range m.Segments[payload.Index] {
		if !ts.Before(seg.TimeMin) && !ts.After(seg.TimeMax) {
			segment = seg
			break
		}
	}

	if segment == nil {
		segment = m.createSegment(payload.Index, ts)
	}

	var doc map[string]string
	err = json.Unmarshal([]byte(payload.Value), &doc)
	if err != nil {
		return fmt.Errorf("invalid document value: %w", err)
	}

	for key, value := range doc {
		segment.Data[value] = append(segment.Data[value], payload.DocId)

		composite := fmt.Sprintf("%s:%s", key, value)
		segment.Data[composite] = append(segment.Data[composite], payload.DocId)
	}

	return nil
}
func (m *MemStorage) createSegment(index string, ts time.Time) *IndexStore {
	seg := &IndexStore{
		Index:   index,
		TimeMin: ts,
		TimeMax: ts.Add(time.Hour),
		Data:    make(map[string][]string),
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	m.Segments[index] = append(m.Segments[index], seg)

	return seg
}

func NewMemStorage(conf *conf.Config) *MemStorage {
	return &MemStorage{
		conf: conf,
		mu:   sync.Mutex{},
	}
}
func (m *MemManager) ConsumeMutations() error {
	fmt.Println("ConsumeMutations")
	messages, err := m.eventHandler.ConsumeTopic(m.conf.Kafka.MutateTopic, "storage-consumer")
	if err != nil {
		return fmt.Errorf("failed to consume to %s: %s", m.conf.Kafka.MutateTopic, err.Error())
	}
	for message := range messages {
		var op string
		for _, header := range message.Headers {
			if header.Key == "op" {
				op = string(header.Value)
			}
			continue
		}
		var payload share.MutatePayload
		err := json.Unmarshal(message.Value, &payload)
		if err != nil {
			return err
		}
		switch share.MutateOp(op) {
		case share.SetOp:
			err := m.storage.Insert(payload)
			if err != nil {
				return err
			}
		default:
			return errors.New("invalid operation")
		}
		fmt.Println("consumed-mutation", string(message.Key), message.Headers)
	}
	return nil
}

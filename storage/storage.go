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

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type StorageManager interface {
	ConsumeMutations() error
	Stats() (*ManagerStats, error)
}
type ManagerStats struct {
	StorageStat *StorageStats
}
type IndexStat struct {
	Index     string
	Slices    int
	Documents string
}
type StorageStats struct {
	Indexes   []IndexStat
	TotalSize int64
}
type Storage interface {
	Insert(payload share.MutatePayload) error
	Stats() (*StorageStats, error)
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

func (m *MemManager) Stats() (*ManagerStats, error) {
	storageStat, err := m.storage.Stats()
	if err != nil {
		return nil, err
	}
	return &ManagerStats{
		StorageStat: storageStat,
	}, nil
}
func (m *MemStorage) Stats() (*StorageStats, error) {

	stats := &StorageStats{
		Indexes:   []IndexStat{},
		TotalSize: 0,
	}

	for index, segments := range m.Segments {
		docIDSet := make(map[string]struct{})
		docCount := 0

		for _, segment := range segments {
			for _, docIDs := range segment.Data {
				for _, id := range docIDs {
					docIDSet[id] = struct{}{}
					docCount++
				}
			}
		}

		stats.Indexes = append(stats.Indexes, IndexStat{
			Index:     index,
			Slices:    len(segments),
			Documents: fmt.Sprintf("%d", len(docIDSet)),
		})

		stats.TotalSize += int64(docCount)
	}

	return stats, nil
}

func (m *MemStorage) Insert(payload share.MutatePayload) error {
	fmt.Printf("Insert payload: %v\n", payload)
	ts, err := time.Parse(time.RFC3339Nano, payload.Timestamp)
	if err != nil {
		return err
	}

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
		conf:     conf,
		mu:       sync.Mutex{},
		Segments: make(map[string][]*IndexStore),
	}
}
func (m *MemManager) ConsumeMutations() error {
	messages, err := m.eventHandler.ConsumeTopic(m.conf.Kafka.MutateTopic, "storage-consumer")
	if err != nil {
		return fmt.Errorf("failed to consume to %s: %s", m.conf.Kafka.MutateTopic, err.Error())
	}
	for message := range messages {
		h := headersMap(message.Headers)

		op := share.MutateOp(h["op"])
		ts := h["ts"]
		docId := h["docId"]

		payload := share.MutatePayload{
			Value:     message.Value,
			Index:     string(message.Key),
			Op:        op,
			Timestamp: ts,
			DocId:     docId,
		}

		switch op {
		case share.SetOp:
			if err := m.storage.Insert(payload); err != nil {
				return err
			}
		default:
			return errors.New("invalid operation")
		}
	}
	return nil
}
func headersMap(hdrs []kafka.Header) map[string]string {
	m := make(map[string]string, len(hdrs))
	for _, h := range hdrs {
		m[h.Key] = string(h.Value)
	}
	return m
}

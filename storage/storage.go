package storage

import (
	"distributed-observer/conf"
	"distributed-observer/event"
	"distributed-observer/share"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type StorageManager interface {
	ConsumeMutations() error
	Stats() (*ManagerStats, error)
	GetStorage() Storage
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
	Search(query share.SearchQuery) (*share.SearchResult, error) // search string that can be key : value , value or key ~: value
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

func (m *MemManager) GetStorage() Storage {
	return m.storage
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

func (m *MemStorage) Search(query share.SearchQuery) (*share.SearchResult, error) {
	filters, err := share.ParseQuery(query.Query)
	if err != nil {
		return nil, fmt.Errorf("failed to parse query: %s", err)
	}
	qMin, err := time.Parse(time.RFC3339Nano, query.TimeMin)
	if err != nil {
		return nil, fmt.Errorf("invalid TimeMin: %w", err)
	}
	qMax, err := time.Parse(time.RFC3339Nano, query.TimeMax)
	if err != nil {
		return nil, fmt.Errorf("invalid TimeMax: %w", err)
	}

	segs := m.Segments[query.Index]
	var rel []*IndexStore
	for _, s := range segs {
		if !s.TimeMin.After(qMax) && !s.TimeMax.Before(qMin) {
			rel = append(rel, s)
		}
	}
	if len(rel) == 0 {
		return &share.SearchResult{}, nil
	}

	merged := mergeSegments(rel)
	data := merged.Data
	docsSet := make(map[string]struct{}, len(data))
	for _, f := range filters {
		var list []string
		switch f.Op {
		case "val":
			list = data[f.Value]
		case "==":
			list = data[f.Field+":"+f.Value]
		default:
			continue
		}
		if f.Exclude {
			for _, id := range list {
				delete(docsSet, id)
			}
		} else {
			for _, id := range list {
				docsSet[id] = struct{}{}
			}
		}
	}

	res := &share.SearchResult{DocumentIds: make([]string, 0, len(docsSet))}
	for id := range docsSet {
		res.DocumentIds = append(res.DocumentIds, id)
	}
	return res, nil
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
func mergeSegments(segs []*IndexStore) *IndexStore {
	if len(segs) == 0 {
		return nil
	}
	sorted := make([]*IndexStore, len(segs))
	copy(sorted, segs)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].TimeMin.Before(sorted[j].TimeMin)
	})

	first := sorted[0]
	merged := &IndexStore{
		Index:   first.Index,
		TimeMin: first.TimeMin,
		TimeMax: first.TimeMax,
		Data:    make(map[string][]string),
	}
	for k, ids := range first.Data {
		merged.Data[k] = append([]string{}, ids...)
	}

	for _, seg := range sorted[1:] {
		if seg.TimeMin.Before(merged.TimeMin) {
			merged.TimeMin = seg.TimeMin
		}
		if seg.TimeMax.After(merged.TimeMax) {
			merged.TimeMax = seg.TimeMax
		}
		for k, ids := range seg.Data {
			merged.Data[k] = append(merged.Data[k], ids...)
		}
	}
	return merged
}

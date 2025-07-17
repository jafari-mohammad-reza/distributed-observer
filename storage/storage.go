package storage

import (
	"bufio"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"observer/conf"
	"observer/share"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/edsrzf/mmap-go"
)

type Storage interface {
	Insert(payload share.MutatePayload) error
	Update(payload share.MutatePayload) (int, error)
	Delete(payload share.MutatePayload) (int, error)
	Search(query share.SearchQuery) (*share.SearchResult, error) // search string that can be key : value , value or key ~: value
	Stats() (*StorageStats, error)
	Init() error
}
type StorageStats struct {
	Indexes   []IndexStat
	TotalSize int64
}
type IndexStat struct {
	Index     string
	Slices    int
	Documents string
}
type IndexStore struct {
	TimeMin time.Time
	TimeMax time.Time
	Index   string
	Data    map[string][]string // value to document ids
}
type WalRecord struct {
	Index     string
	Timestamp int64
	Key       string
	DocID     string
}

type MemStorage struct {
	mu       sync.Mutex
	conf     *conf.Config
	Segments map[string][]*IndexStore
	walCh    chan WalRecord
}

func NewMemStorage(conf *conf.Config) *MemStorage {
	return &MemStorage{
		conf:     conf,
		mu:       sync.Mutex{},
		Segments: make(map[string][]*IndexStore),
		walCh:    make(chan WalRecord, 1_000_000),
	}
}
func (m *MemStorage) Init() error {
	f, _ := os.OpenFile(m.conf.Storage.WalPath, os.O_RDONLY, 0)
	data, _ := mmap.Map(f, mmap.RDONLY, 0)
	defer data.Unmap()
	offset := 0
	for offset+4 <= len(data) {
		size := int(binary.BigEndian.Uint32(data[offset : offset+4]))
		offset += 4
		recBytes := data[offset : offset+size]
		offset += size
		var rec WalRecord
		json.Unmarshal(recBytes, &rec)
		t := time.Unix(0, rec.Timestamp)
		var seg *IndexStore
		for _, s := range m.Segments[rec.Index] {
			if !t.Before(s.TimeMin) && !t.After(s.TimeMax) {
				seg = s
				break
			}
		}
		if seg == nil {
			seg = m.createSegment(rec.Index, t)
		}
		if seg.Data == nil {
			seg.Data = make(map[string][]string)
		}
		seg.Data[rec.Key] = append(seg.Data[rec.Key], rec.DocID)
		if t.Before(seg.TimeMin) {
			seg.TimeMin = t
		}
		if t.After(seg.TimeMax) {
			seg.TimeMax = t
		}
	}
	go func() {
		f, _ := os.OpenFile(m.conf.Storage.WalPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
		bufw := bufio.NewWriterSize(f, 1<<20)
		defer f.Close()
		for rec := range m.walCh {
			b, err := json.Marshal(rec)
			if err != nil {
				fmt.Printf("writing val err: %v\n", err.Error()) // TODO: push this to event handler to try to rewrite the value DLQ
				return
			}
			binary.Write(bufw, binary.BigEndian, uint32(len(b)))
			bufw.Write(b)
			bufw.Flush()

			if bufw.Buffered() > 512<<10 {
				bufw.Flush()
			}
		}
	}()
	return nil
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

		walRec := WalRecord{
			Index:     payload.Index,
			Timestamp: ts.UnixNano(),
			DocID:     payload.DocId,
			Key:       composite,
		}
		m.walCh <- walRec
	}

	return nil
}
func (m *MemStorage) Delete(payload share.MutatePayload) (int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	segments := m.Segments[payload.Index]
	deletedCount := 0

	for _, seg := range segments {
		for key, docs := range seg.Data {
			fmt.Printf("key: %v\n", key)
			filtered := docs[:0]
			for _, docID := range docs {
				fmt.Printf("docID: %v\n", docID)
				if docID == payload.DocId {
					deletedCount++
				} else {
					filtered = append(filtered, docID)
				}
			}
			if len(filtered) == 0 {
				delete(seg.Data, key)
			} else {
				seg.Data[key] = filtered
			}
			if len(seg.Data) == 0 {
				delete(m.Segments, seg.Index)
			}
		}
	}
	return deletedCount / 2, nil // as we both save the value and key value
}
func (m *MemStorage) Update(payload share.MutatePayload) (int, error) {
	deletedCount, err := m.Delete(payload)
	if err != nil {
		return 0, err
	}

	if err := m.Insert(payload); err != nil {
		return deletedCount, err
	}

	return deletedCount, nil
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
		case share.DelOp:
			count, err := m.storage.Delete(payload)
			if err != nil {
				return err
			}
			fmt.Printf("del count: %v\n", count)
		case share.UpdateOp:
			count, err := m.storage.Update(payload)
			if err != nil {
				return err
			}
			fmt.Printf("update count: %v\n", count)
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

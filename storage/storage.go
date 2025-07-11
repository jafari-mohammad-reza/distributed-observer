package storage

import (
	"distributed-observer/conf"
	"distributed-observer/event"
	"fmt"
	"sync"
	"time"
)

type StorageManager interface {
	ConsumeMutations() error
}
type Storage interface {
	Insert() error
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

func (m *MemStorage) Insert() error {
	return nil
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
		fmt.Println("consumed-mutation", string(message.Key), message.Headers)
	}
	return nil
}

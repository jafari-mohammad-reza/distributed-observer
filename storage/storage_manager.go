package storage

import (
	"distributed-observer/conf"
	"distributed-observer/event"
	"distributed-observer/server"
	"distributed-observer/share"
	"encoding/json"
	"fmt"
)

type StorageManager interface {
	Start() error
	Stats() (*ManagerStats, error)
}
type ManagerStats struct {
	StorageStat *StorageStats
}
type MemManager struct {
	conf         *conf.Config
	storage      Storage
	eventHandler event.EventHandler
	tcpServer    server.Server
}

func NewStorageManager(conf *conf.Config, eventHandler event.EventHandler) StorageManager {
	manager := MemManager{
		conf:         conf,
		storage:      NewMemStorage(conf),
		eventHandler: eventHandler,
	}
	manager.tcpServer = server.NewServer(conf, eventHandler, manager.handleCommand)
	return &manager
}
func (m *MemManager) handleCommand(packet *share.TransferPacket) {
	payload := packet.Payload
	conn := *packet.Conn
	switch packet.Command {
	case share.SearchCommand:
		var SearchQuery share.SearchQuery
		err := json.Unmarshal(payload, &SearchQuery)
		if err != nil {
			fmt.Println("error parsing search query")
			conn.Write([]byte(err.Error()))
			return
		}
		result, err := m.storage.Search(SearchQuery)
		if err != nil {
			fmt.Println("failed searching for search query")
			conn.Write([]byte(err.Error()))
			return
		}
		fmt.Printf("result: %v\n", result)
		fmt.Fprintf(conn, "found docs count:%d", len(result.DocumentIds))
	default:
		conn.Write([]byte("invalid command"))
	}
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

func (m *MemManager) Start() error {
	var startErr error
	go func() {
		if err := m.ConsumeMutations(); err != nil {
			startErr = err
		}
	}()
	go func() {
		if err := m.tcpServer.Start(m.conf.Storage.Port); err != nil {
			startErr = err
		}
	}()
	startErr = m.storage.Init()
	if startErr != nil {
		return startErr
	}
	return nil
}

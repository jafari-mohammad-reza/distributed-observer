package storage

import (
	"distributed-observer/conf"
	"distributed-observer/event"
	"distributed-observer/server"
	"distributed-observer/share"
	"encoding/json"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
)

func TestStorageManager(t *testing.T) {
	config := &conf.Config{
		Port: 8081,
		Kafka: conf.KafkaConf{
			Brokers:        "localhost:9092",
			ClientId:       "test-client",
			LogTopic:       "test-logs",
			LogChanSize:    100,
			MutateChanSize: 100,
			MutateTopic:    "test-mutates",
		},
	}
	handler := event.NewEventHandler(config)
	err := handler.Connect()
	assert.Nil(t, err, "Event handler should connect without error")
	server := server.NewServer(config, handler)
	go func() {
		err := server.Start()
		assert.Nil(t, err, "Server should start without error")
	}()

	conn, err := net.Dial("tcp", "localhost:8081")
	assert.Nil(t, err, "Should connect to server without error")
	message, err := json.Marshal(struct {
		Name   string `json:"name"`
		Family string `json:"family"`
	}{
		Name:   "test",
		Family: "test-family",
	})
	assert.Nil(t, err)
	setPayload := share.MutatePayload{
		Op:    share.SetOp,
		Index: "logs-2025-12",
		Value: message,
		DocId: uuid.NewString(),
	}
	payload, err := json.Marshal(setPayload)
	assert.Nil(t, err, "the payload should be marshalled without error")
	setPacket := share.TransferPacket{
		Command:  share.SetCommand,
		Sender:   "test-sender",
		Receiver: "test-receiver",
		Time:     time.Now(),
		Headers:  map[string]string{"test-header": "test-value"},
		Payload:  payload,
	}
	serializedPacket, err := share.SerializeTransferPacket(&setPacket)
	assert.Nil(t, err, "Should serialize packet without error")
	err = share.SendDataOverTcp(conn, int64(len(serializedPacket)), serializedPacket)
	assert.Nil(t, err, "Should send data over TCP without error")

	storageManager := NewStorageManager(config, handler)
	go func() {
		if err := storageManager.ConsumeMutations(); err != nil {
			assert.Nil(t, err, "consumeMutations should return nil as error")
		}
	}()
	time.Sleep(time.Second * 10)
	managerStats, err := storageManager.Stats()
	assert.Nil(t, err, "storage manager status error should be nil")
	fmt.Printf("managerStats.StorageStat.Indexes: %v\n", managerStats.StorageStat.Indexes)
	assert.Greater(t, len(managerStats.StorageStat.Indexes), 0, "storage indexes length must be more than zero")

}

func TestStorage(t *testing.T) {

	config := &conf.Config{
		Port: 8081,
		Kafka: conf.KafkaConf{
			Brokers:        "localhost:9092",
			ClientId:       "test-client",
			LogTopic:       "test-logs",
			LogChanSize:    100,
			MutateChanSize: 100,
			MutateTopic:    "test-mutates",
		},
	}
	storage := NewMemStorage(config)

	message, err := json.Marshal(struct {
		Name   string `json:"name"`
		Family string `json:"family"`
	}{
		Name:   "test",
		Family: "test-family",
	})
	assert.Nil(t, err)
	creationTs := time.Now().Format(time.RFC3339Nano)
	err = storage.Insert(share.MutatePayload{
		Op:        share.SetOp,
		Index:     "test",
		Timestamp: creationTs,
		DocId:     "test-doc",
		Value:     message,
	})
	assert.Nil(t, err)
	stats, err := storage.Stats()

	assert.Nil(t, err)
	assert.Greater(t, len(stats.Indexes), 0)
	searchQuery := share.SearchQuery{
		Index:   "test",
		TimeMin: creationTs,
		TimeMax: time.Now().Add(time.Hour).Format(time.RFC3339Nano),
		Query:   "name == test",
	}
	res, err := storage.Search(searchQuery)

	assert.Nil(t, err)
	fmt.Printf("res: %v\n", res)
	assert.Greater(t, len(res.DocumentIds), 0)
	searchQuery = share.SearchQuery{
		Index:   "test",
		TimeMin: creationTs,
		TimeMax: time.Now().Add(time.Hour).Format(time.RFC3339Nano),
		Query:   "name == test NOT family == test-family",
	}
	res, err = storage.Search(searchQuery)

	assert.Nil(t, err)
	fmt.Printf("res: %v\n", res)
	assert.Equal(t, len(res.DocumentIds), 0)
	searchQuery = share.SearchQuery{
		Index:   "test",
		TimeMin: creationTs,
		TimeMax: time.Now().Add(time.Hour).Format(time.RFC3339Nano),
		Query:   "name == test AND family == test-family",
	}
	res, err = storage.Search(searchQuery)

	assert.Nil(t, err)
	fmt.Printf("res: %v\n", res)
	assert.Equal(t, len(res.DocumentIds), 1)
	// TODO: test value search
}

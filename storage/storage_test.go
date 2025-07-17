package storage

import (
	"encoding/json"
	"fmt"
	"observer/conf"
	"observer/share"
	"path"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestStorage(t *testing.T) {
	tmpDir := t.TempDir()
	walPath := path.Join(tmpDir, "data.wal")
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
		Storage: conf.StorageConf{
			Port:    8082,
			WalPath: walPath,
		},
	}
	storage := NewMemStorage(config)
	storage.Init()
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
	assert.Greater(t, len(storage.walCh), 0, "wal chan should have value")
	time.Sleep(time.Second)
	assert.Equal(t, len(storage.walCh), 0, "wal chan should be empty")
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
	assert.Greater(t, len(res.DocumentIds), 0)
	searchQuery = share.SearchQuery{
		Index:   "test",
		TimeMin: creationTs,
		TimeMax: time.Now().Add(time.Hour).Format(time.RFC3339Nano),
		Query:   "name == test NOT family == test-family",
	}
	res, err = storage.Search(searchQuery)

	assert.Nil(t, err)
	assert.Equal(t, len(res.DocumentIds), 0)
	searchQuery = share.SearchQuery{
		Index:   "test",
		TimeMin: creationTs,
		TimeMax: time.Now().Add(time.Hour).Format(time.RFC3339Nano),
		Query:   "name == test AND family == test-family",
	}
	res, err = storage.Search(searchQuery)

	assert.Nil(t, err)
	assert.Equal(t, len(res.DocumentIds), 1)
	searchQuery = share.SearchQuery{
		Index:   "test",
		TimeMin: creationTs,
		TimeMax: time.Now().Add(time.Hour).Format(time.RFC3339Nano),
		Query:   "test",
	}

	res, err = storage.Search(searchQuery)

	assert.Nil(t, err)
	assert.Equal(t, len(res.DocumentIds), 1)

	searchQuery = share.SearchQuery{
		Index:   "test",
		TimeMin: creationTs,
		TimeMax: time.Now().Add(time.Hour).Format(time.RFC3339Nano),
		Query:   "test NOT test-family",
	}

	res, err = storage.Search(searchQuery)

	assert.Nil(t, err)
	assert.Equal(t, len(res.DocumentIds), 0)
	delPayload := share.MutatePayload{
		Op:        share.DelOp,
		Index:     "test",
		Timestamp: time.Now().Format(time.RFC3339Nano),
		DocId:     "test-doc",
		Value:     nil,
	}
	count, err := storage.Delete(delPayload)
	fmt.Printf("count: %v\n", count)
	assert.Nil(t, err)
	assert.Equal(t, count, 2) // as the document had two fields
	stats, err = storage.Stats()
	assert.Nil(t, err)
	assert.Equal(t, len(stats.Indexes), 0)
}

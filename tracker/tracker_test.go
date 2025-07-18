package tracker

import (
	"observer/conf"
	"observer/event"
	"observer/share"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTracker(t *testing.T) {
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
		Tracker: conf.TrackerConf{
			Port: 8082,
		},
	}
	tracker := NewTracker(config, event.NewEventHandler(config))
	transactionID, err := tracker.StartTransaction(share.StartTransactionPayload{
		Name: "test-transaction",
		Headers: map[string]string{
			"test-header": "test-value",
		},
	})
	assert.Nil(t, err)
	assert.NotEqual(t, transactionID, "")
	stat := tracker.Stats()
	assert.Equal(t, stat.ActiveTransactionsCount, 1)
	assert.Equal(t, stat.ActiveTransactions[0].Name, "test-transaction")
	assert.Equal(t, stat.FinishedTransactionsCount, 0)
	spanID, err := tracker.StartSpan(share.StartSpanPayload{
		Name:     "test-span",
		ParentID: transactionID,
	})
	stat = tracker.Stats()
	assert.Nil(t, err)
	assert.NotEqual(t, spanID, "")
	assert.Equal(t, len(stat.ActiveTransactions[0].Spans), 1)
	assert.Equal(t, stat.ActiveSpansCount, 1)

	_, err = tracker.StartSpan(share.StartSpanPayload{
		Name:     "test-span",
		ParentID: "undefined-transaction-id",
	})
	assert.NotNil(t, err)
	assert.Equal(t, err.Error(), "transaction not found")
	err = tracker.EndTransaction(transactionID)
	stat = tracker.Stats()
	assert.Nil(t, err)
	assert.Equal(t, stat.ActiveTransactionsCount, 0)
	assert.Equal(t, stat.ActiveSpansCount, 0)
	assert.Equal(t, stat.FinishedTransactionsCount, 1)
	err = tracker.EndTransaction("undefined-transaction")
	assert.NotNil(t, err)
	assert.Equal(t, err.Error(), "transaction not found")
}

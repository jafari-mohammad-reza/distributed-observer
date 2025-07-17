package observer

import (
	"encoding/json"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"distributed-observer/conf"
	"distributed-observer/event"
	"distributed-observer/share"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type fakeEventHandler struct {
	mutated bool

	payload share.MutatePayload
}

func (f *fakeEventHandler) Connect() error {
	return nil
}

func (f *fakeEventHandler) Disconnect() error {
	return nil
}

func (f *fakeEventHandler) Log(level event.LogType, msg string) error {

	return nil
}

func (f *fakeEventHandler) Mutate(payload share.MutatePayload) {
	f.mutated = true
	f.payload = payload
}

func (f *fakeEventHandler) ConsumeTopic(topic, groupId string) (chan *kafka.Message, error) {
	ch := make(chan *kafka.Message)
	close(ch)
	return ch, nil
}

func TestHandleCommand_SetCommand(t *testing.T) {

	handler := &fakeEventHandler{}
	o := NewObserver(&conf.Config{}, handler)

	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	payload := share.MutatePayload{
		Index:     "test-index",
		DocId:     "doc1",
		Timestamp: time.Now().Format(time.RFC3339Nano),
		Value:     json.RawMessage(`{"foo":"bar"}`),
		Op:        share.SetOp,
	}
	payloadBytes, err := json.Marshal(payload)
	assert.NoError(t, err, "failed to marshal payload")

	packet := &share.TransferPacket{
		Command: share.SetCommand,
		Payload: payloadBytes,
		Conn:    &server,
	}

	go o.handleCommand(packet)

	respBytes, err := share.ReadConn(client, time.Now().Add(time.Second))
	assert.NoError(t, err, "failed to read response")

	resp := string(respBytes)
	const expected = "set command applied"
	assert.Equal(t, expected, resp, "unexpected response")

	assert.True(t, handler.mutated, "Mutate should be called")
	assert.Equal(t, payload, handler.payload, "unexpected payload")
}

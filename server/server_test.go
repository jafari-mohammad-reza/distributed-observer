package server

import (
	"distributed-observer/conf"
	"distributed-observer/event"
	"distributed-observer/share"
	"encoding/binary"
	"io"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestServer(t *testing.T) {
	config := &conf.Config{
		Port: 8080,
		Kafka: conf.KafkaConf{
			Brokers:     "localhost:9092",
			ClientId:    "test-client",
			LogTopic:    "test-logs",
			LogChanSize: 100,
		},
	}
	handler := event.NewEventHandler(config)
	err := handler.Connect()
	assert.Nil(t, err, "Event handler should connect without error")
	server := NewServer(config, handler)
	go func() {
		err := server.Start()
		assert.Nil(t, err, "Server should start without error")
	}()
	time.Sleep(time.Second)
	conn, err := net.Dial("tcp", "localhost:8080")
	assert.Nil(t, err, "Should connect to server without error")

	setPacket := share.TransferPacket{
		Sender:   "test-sender",
		Receiver: "test-receiver",
		Time:     time.Now(),
		Headers:  map[string]string{"test-header": "test-value"},
		Payload:  []byte("test data"),
		Command:  share.CommandSet,
	}
	serializedPacket, err := share.SerializeTransferPacket(&setPacket)
	assert.Nil(t, err, "Should serialize packet without error")
	err = share.SendDataOverTcp(conn, int64(len(serializedPacket)), serializedPacket)
	assert.Nil(t, err, "Should send data over TCP without error")
	var size int64
	err = binary.Read(conn, binary.BigEndian, &size)
	assert.Nil(t, err)

	buf := make([]byte, size)
	_, err = io.ReadFull(conn, buf)
	assert.Nil(t, err)

	assert.Equal(t, string(buf), "set command applied")
	defer conn.Close()
}

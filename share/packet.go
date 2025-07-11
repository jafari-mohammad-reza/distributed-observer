package share

import (
	"bytes"
	"encoding/gob"
	"net"
	"time"
)

type Command string

const (
	CommandSet    Command = "SET"
	CommandSearch Command = "SEARCH"
	CommandDelete Command = "DELETE"
)

type TransferPacket struct {
	Command  Command
	Payload  []byte
	Headers  map[string]string
	Time     time.Time
	Sender   string
	Receiver string
	Conn *net.Conn
}

func DeserializeTransferPacket(data []byte) (*TransferPacket, error) {
	var packet TransferPacket
	buf := bytes.NewBuffer(data)
	decoder := gob.NewDecoder(buf)
	if err := decoder.Decode(&packet); err != nil {
		return nil, err
	}
	return &packet, nil
}
func SerializeTransferPacket(packet *TransferPacket) ([]byte, error) {
	var buf bytes.Buffer
	encoder := gob.NewEncoder(&buf)
	if err := encoder.Encode(packet); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

package share

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"net"
	"time"
)

type Command string

const (
	SetCommand    Command = "SET"
	SearchCommand Command = "SEARCH"
	DeleteCommand Command = "DELETE"
)

type TransferPacket struct {
	Command  Command
	Payload  []byte
	Headers  map[string]string
	Time     time.Time
	Sender   string
	Receiver string
	Conn     *net.Conn
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

type MutateOp string

const (
	SetOp    MutateOp = "SET"
	DelOp    MutateOp = "DEL"
	UpdateOp MutateOp = "UPDATE"
)

type MutatePayload struct {
	Op        MutateOp        `json:"op"`    // "set" , "delete" , "update"
	Index     string          `json:"index"` // "logs-2025-07"
	Value     json.RawMessage `json:"value,omitempty"`
	DocId     string
	Timestamp string `json:"time_stamp"`
}

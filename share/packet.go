package share

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"fmt"
	"net"
	"strings"
	"time"
)

type Command string

const (
	SetCommand    Command = "SET"
	SearchCommand Command = "SEARCH"
	DeleteCommand Command = "DELETE"
	UpdateCommand Command = "UPDATE"
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
	SearchOp MutateOp = "SEARCH"
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

type SearchQuery struct {
	Index   string `json:"index"`
	Query   string `json:"query"` // ex: key == value AND value NOT key ~= value
	TimeMin string `json:"timeMin"`
	TimeMax string `json:"timeMax"`
}

type SearchFilter struct {
	Field   string // e.g. "key"
	Op      string // "==" or "~="
	Value   string // e.g. "value"
	Exclude bool   // true if prefixed with NOT
}

func ParseQuery(q string) ([]SearchFilter, error) {
	parts := strings.Split(q, "AND")
	fmt.Printf("parts: %v\n", parts)
	var out []SearchFilter
	for _, raw := range parts {
		part := strings.TrimSpace(raw)

		f := SearchFilter{}
		if strings.HasPrefix(part, "NOT ") {
			f.Exclude = true
			part = strings.TrimSpace(strings.TrimPrefix(part, "NOT "))
		}

		var op string
		switch {
		case strings.Contains(part, "=="):
			op = "=="
		case strings.Contains(part, "~="):
			op = "~="
		case !strings.Contains(part, "==") && !strings.Contains(part, "~="):
			op = "val"
		default:
			return nil, fmt.Errorf("unknown operator in clause %q", part)
		}
		f.Op = op

		tokens := strings.SplitN(part, op, 2)
		if op != "val" {
			f.Field = strings.TrimSpace(tokens[0])
			f.Value = strings.TrimSpace(tokens[1])
		} else {
			f.Field = ""
			f.Value = strings.TrimSpace(tokens[0])
		}

		f.Value = strings.Trim(f.Value, `"'`)
		out = append(out, f)
	}
	return out, nil
}

type SearchResult struct {
	DocumentIds []string `json:"document_ids"`
}

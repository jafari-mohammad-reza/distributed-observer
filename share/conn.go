package share

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"net"
)

func RequestConn(conn net.Conn, size int64, dataBytes []byte) error {
	err := binary.Write(conn, binary.BigEndian, size)
	if err != nil {

		return fmt.Errorf("failed to write size: %w", err)
	}
	_, err = io.CopyN(conn, bytes.NewReader(dataBytes), size)
	if err != nil {
		return fmt.Errorf("failed to write data: %w", err)
	}
	return nil
}

func RespondConn(conn net.Conn, msg []byte) error {
	err := binary.Write(conn, binary.BigEndian, int64(len(msg)))
	if err != nil {
		return err
	}
	_, err = conn.Write(msg)
	if err != nil {
		return err
	}
	return nil
}

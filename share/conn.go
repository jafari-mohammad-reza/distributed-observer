package share

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"time"
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

func ReadConn(conn net.Conn, deadline time.Time) ([]byte, error) {
	buff := new(bytes.Buffer)
	var size int64
	err := conn.SetReadDeadline(deadline)
	if err != nil {
		return nil, err
	}
	err = binary.Read(conn, binary.BigEndian, &size)
	if err != nil {
		return nil, err
	}
	_, err = io.CopyN(buff, conn, size)
	if err != nil {
		return nil, err
	}
	return buff.Bytes(), nil
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

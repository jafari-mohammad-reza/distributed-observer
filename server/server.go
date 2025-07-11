package server

import (
	"bytes"
	"distributed-observer/conf"
	"distributed-observer/event"
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"io"
	"net"
	"time"
)

type Server interface {
	Start() error
}

func NewServer(config *conf.Config, eventHandler event.EventHandler) Server {
	return &TCPServer{config: config, eventHandler: eventHandler}
}

type TCPServer struct {
	config       *conf.Config
	eventHandler event.EventHandler
}
type TransferPacket struct {
	Payload  []byte
	Headers  map[string]string
	Time     time.Time
	Sender   string
	Receiver string
}

func (s *TCPServer) Start() error {
	ln, err := net.Listen("tcp", fmt.Sprintf(":%d", s.config.Port))
	if err != nil {
		return err
	}
	defer ln.Close()
	s.eventHandler.Log(event.InfoLog, fmt.Sprintf("Server started on port %d", s.config.Port))
	for {
		conn, err := ln.Accept()
		if err != nil {
			s.eventHandler.Log(event.ErrorLog, fmt.Sprintf("failed to accept connection: %s", err.Error()))
		}
		go s.handleConn(conn)
	}
}
func (s *TCPServer) handleConn(conn net.Conn) error {
	defer conn.Close()
	s.eventHandler.Log(event.InfoLog, "New connection established")
	buf := new(bytes.Buffer)
	var size int64
	err := conn.SetReadDeadline(time.Now().Add(5 * time.Second)) //TODO: make deadline optional and customizable
	if err != nil {
		return s.eventHandler.Log(event.ErrorLog, fmt.Sprintf("failed to set read deadline: %s", err.Error()))

	}
	err = binary.Read(conn, binary.BigEndian, &size)
	if err != nil {
		return s.eventHandler.Log(event.ErrorLog, fmt.Sprintf("failed to read size: %s", err.Error()))

	}
	_, err = io.CopyN(buf, conn, size)
	if err != nil {
		return s.eventHandler.Log(event.ErrorLog, fmt.Sprintf("failed to read data: %s", err.Error()))

	}
	des, err := s.deserializePacket(buf.Bytes())
	if err != nil {
		return s.eventHandler.Log(event.ErrorLog, fmt.Sprintf("failed to deserialize packet: %s", err.Error()))

	}
	s.eventHandler.Log(event.InfoLog, fmt.Sprintf("Received packet from %s to %s at %s with headers: %v", des.Sender, des.Receiver, des.Time, des.Headers))
	return nil
}
func (s *TCPServer) deserializePacket(data []byte) (*TransferPacket, error) {
	var packet TransferPacket
	buf := bytes.NewBuffer(data)
	decoder := gob.NewDecoder(buf)
	if err := decoder.Decode(&packet); err != nil {
		return nil, err
	}
	return &packet, nil
}

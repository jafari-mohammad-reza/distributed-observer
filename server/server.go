package server

import (
	"bytes"
	"distributed-observer/conf"
	"distributed-observer/event"
	"distributed-observer/share"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"time"
)

type Server interface {
	Start(port int) error
}

type HandleCommand func(packet *share.TransferPacket)

func NewServer(config *conf.Config, eventHandler event.EventHandler, hc HandleCommand) Server {
	return &TCPServer{config: config, eventHandler: eventHandler, handleCommand: hc}
}

// TODO: export TCP server as shared package and each server will have its own command handler this package name will be observer
type TCPServer struct {
	config        *conf.Config
	eventHandler  event.EventHandler
	handleCommand HandleCommand
}

func (s *TCPServer) Start(port int) error {
	ln, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		return err
	}
	defer ln.Close()
	s.eventHandler.Log(event.InfoLog, fmt.Sprintf("Server started on port %d", port))
	for {
		conn, err := ln.Accept()
		if err != nil {
			s.eventHandler.Log(event.ErrorLog, fmt.Sprintf("failed to accept connection: %s", err.Error()))
		}
		go s.handleConn(conn)
	}
}
func (s *TCPServer) handleConn(conn net.Conn) error {
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
	des, err := share.DeserializeTransferPacket(buf.Bytes())
	if err != nil {
		return s.eventHandler.Log(event.ErrorLog, fmt.Sprintf("failed to deserialize packet: %s", err.Error()))
	}
	s.eventHandler.Log(event.InfoLog, fmt.Sprintf("Received packet from %s to %s at %s with headers: %v", des.Sender, des.Receiver, des.Time, des.Headers))
	des.Conn = &conn
	go s.handleCommand(des)
	return nil
}

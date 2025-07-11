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
	Start() error
}

func NewServer(config *conf.Config, eventHandler event.EventHandler) Server {
	return &TCPServer{config: config, eventHandler: eventHandler}
}

type TCPServer struct {
	config       *conf.Config
	eventHandler event.EventHandler
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
func (s *TCPServer) handleCommand(packet *share.TransferPacket) {
	conn := *packet.Conn
	defer conn.Close()
	switch packet.Command {
	case share.CommandSet:
		s.eventHandler.Log(event.InfoLog, "Handling SET command for packet")
		s.respondConn(conn, []byte("set command applied"))
	default:
		s.eventHandler.Log(event.ErrorLog, fmt.Sprintf("Unknown command: %s", packet.Command))
		return
	}
}

func (s *TCPServer) respondConn(conn net.Conn, msg []byte) error {
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

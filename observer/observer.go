package observer

import (
	"distributed-observer/conf"
	"distributed-observer/event"
	"distributed-observer/server"
	"distributed-observer/share"
	"encoding/json"
	"fmt"
	"net"
	"time"
)

type Observer struct {
	conf         *conf.Config
	eventHandler event.EventHandler
	tcpServer    server.Server
}

// TODO: refactor event handler usage
func NewObserver(conf *conf.Config, eventHandler event.EventHandler) *Observer {
	return &Observer{
		conf:         conf,
		eventHandler: eventHandler,
	}
}

func (o *Observer) Start() error {
	var start_err error
	o.tcpServer = server.NewServer(o.conf, o.eventHandler, o.handleCommand)
	go func() {
		err := o.tcpServer.Start(o.conf.Port)
		if err != nil {
			start_err = err
		}
	}()
	go func() {

	}()
	if start_err != nil {
		return start_err
	}
	return nil
}

func (s *Observer) handleCommand(packet *share.TransferPacket) {
	conn := *packet.Conn
	defer conn.Close()
	fmt.Printf("command----%s", packet.Command)
	switch packet.Command {
	case share.SetCommand:
		var payload share.MutatePayload
		err := json.Unmarshal(packet.Payload, &payload)
		if err != nil {
			s.eventHandler.Log(event.ErrorLog, fmt.Sprintf("failed to unmarshal SET command payload: %s", err.Error()))
		}
		s.eventHandler.Mutate(payload)
		share.RespondConn(conn, []byte("set command applied"))
	case share.SearchCommand:
		serializedPacket, err := share.SerializeTransferPacket(packet)
		if err != nil {
			fmt.Printf("err.Error(): %v\n", err.Error())
			share.RespondConn(conn, []byte(err.Error()))
			return
		}
		storageConn, err := net.Dial("tcp", fmt.Sprintf(":%d", s.conf.Storage.Port))
		if err != nil {
			fmt.Printf("err.Error(): %v\n", err.Error())
			share.RespondConn(conn, []byte(err.Error()))
			return
		}
		err = share.RequestConn(storageConn, int64(len(serializedPacket)), serializedPacket)
		if err != nil {
			fmt.Printf("err.Error(): %v\n", err.Error())
			share.RespondConn(conn, []byte(err.Error()))
			return
		}
		resp, err := share.ReadConn(storageConn, time.Now().Add(time.Second*30))
		if err != nil {
			fmt.Printf("err.Error(): %v\n", err.Error())
			share.RespondConn(conn, []byte(err.Error()))
			return
		}
		fmt.Printf("resp: %s\n", string(resp))
		err = share.RespondConn(conn, resp)
		if err != nil {
			fmt.Printf("err.Error(): %v\n", err.Error())
			share.RespondConn(conn, []byte(err.Error()))
			return
		}
	default:
		s.eventHandler.Log(event.ErrorLog, fmt.Sprintf("Unknown command: %s", packet.Command))
		return
	}
}

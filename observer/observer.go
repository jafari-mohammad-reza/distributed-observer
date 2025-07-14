package observer

import (
	"distributed-observer/conf"
	"distributed-observer/event"
	"distributed-observer/server"
	"distributed-observer/share"
	"encoding/json"
	"fmt"
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
	o.tcpServer = server.NewServer(o.conf, o.eventHandler, o.handleCommand)
	err := o.tcpServer.Start(o.conf.Port)
	if err != nil {
		return err
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
		var payload share.SearchQuery
		err := json.Unmarshal(packet.Payload, &payload)
		if err != nil {
			s.eventHandler.Log(event.ErrorLog, fmt.Sprintf("failed to unmarshal SEARCH command payload: %s", err.Error()))
		}
		// TODO: send search query to storage port
		// search, err := s.storage.Search(payload)
		// if err != nil {
		// 	s.respondConn(conn, []byte(err.Error()))
		// }
		// res, err := json.Marshal(search)
		// if err != nil {
		// 	s.respondConn(conn, []byte(err.Error()))
		// }
		// s.respondConn(conn, res)
	default:
		s.eventHandler.Log(event.ErrorLog, fmt.Sprintf("Unknown command: %s", packet.Command))
		return
	}
}

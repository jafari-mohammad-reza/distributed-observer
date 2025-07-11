package main

import (
	"distributed-observer/conf"
	"distributed-observer/event"
	"distributed-observer/server"
	"fmt"
)

func main() {
	conf, err := conf.NewConfig()
	if err != nil {
		panic(fmt.Sprintf("error loading configurations: %s", err.Error()))
	}
	eventHandler := event.NewEventHandler(conf)
	if err := eventHandler.Connect(); err != nil {
		panic(fmt.Sprintf("error connecting to event handler: %s", err.Error()))
	}
	server := server.NewServer(conf, eventHandler)
	if err := server.Start(); err != nil {
		panic(fmt.Sprintf("error starting server: %s", err.Error()))
	}
}

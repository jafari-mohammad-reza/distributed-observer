package main

import (
	"distributed-observer/conf"
	"distributed-observer/event"
	"distributed-observer/server"
	"distributed-observer/storage"
	"fmt"
	"os"
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

	storageManager := storage.NewStorageManager(conf, eventHandler)
	go func() {
		if err := storageManager.ConsumeMutations(); err != nil {
			err := eventHandler.Log(event.ErrorLog, fmt.Sprintf("error consuming mutations: %s", err.Error()))
			if err != nil {
				os.Exit(1)
			}
		}
	}()
	observer := server.NewServer(conf, eventHandler, storageManager)
	if err := observer.Start(); err != nil {
		err := eventHandler.Log(event.ErrorLog, fmt.Sprintf("error starting observer: %s", err.Error()))
		if err != nil {
			os.Exit(1)
		}
	}
}

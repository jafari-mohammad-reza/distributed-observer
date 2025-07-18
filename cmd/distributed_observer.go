package main

import (
	"fmt"
	"observer/conf"
	"observer/event"
	"observer/observer"
	"observer/storage"
	"observer/tracker"
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
	logger := event.NewLogServer(conf, eventHandler)
	logger.Start()
	storageManager := storage.NewStorageManager(conf, eventHandler)
	go func() {
		if err := storageManager.Start(); err != nil {
			err := eventHandler.Log(event.ErrorLog, fmt.Sprintf("error starting storage manager: %s", err.Error()))
			if err != nil {
				os.Exit(1)
			}
		}
	}()
	tr := tracker.NewTracker(conf, eventHandler)
	go func() {
		if err := tr.Start(); err != nil {
			err := eventHandler.Log(event.ErrorLog, fmt.Sprintf("error starting tracker server: %s", err.Error()))
			if err != nil {
				os.Exit(1)
			}
		}
	}()
	obr := observer.NewObserver(conf, eventHandler)
	if err := obr.Start(); err != nil {
		os.Exit(1)
	}
}

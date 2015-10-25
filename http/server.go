package main

import (
	"encoding/json"
    eatonconfig "github.com/ECLabs/Eaton-Feeder/config"
	eatonevents "github.com/ECLabs/Eaton-Feeder/events"
	"log"
	"net/http"
)

func main() {
    eatonconfig.OffsetType = "oldest"
	consumer, err := eatonevents.NewEventConsumer()
	if err != nil {
		log.Fatal("failed to create kafka consumer: ", err)
	}
	out, outErr := consumer.Consume()
	go func() {
		for msg := range out {
			data, err := json.Marshal(msg)
			if err != nil {
				log.Println("failed to marshal msg: ", err)
				continue
			}
			h.broadcast <- data
		}
	}()
	go func() {
		for err := range outErr {
			log.Println("error: ", err)
		}
	}()
	go h.run()
	http.HandleFunc("/ws", serveWs)
	http.Handle("/", http.FileServer(http.Dir("./assets")))
	log.Println("Serving at localhost:5000...")
	log.Fatal(http.ListenAndServe(":5000", nil))
}

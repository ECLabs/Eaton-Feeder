package main

import (
	eatonevents "github.com/ECLabs/Eaton-Feeder/events"
	"log"
	"net/http"
)

func main() {
	server, err := socketio.NewServer(nil)
	if err != nil {
		log.Fatal(err)
	}
    consumer, err := eatonevents.NewEventConsumer()
	if err != nil {
		log.Fatal("failed to create kafka consumer: ", err)
	}
	out, outErr := consumer.Consume()
	
	server.On("connection", func(so socketio.Socket) {
        log.Println("socket connected!")
		so.Join("logs")
		so.On("enter room", func(room string) {
			so.Join(room)
		})
        so.On("leave", func(room string){
            so.Leave(room)
        })
        so.On("test", func(){
            err := so.Emit("logs", "hey")
            if err != nil {
                log.Println("failed to emit: ", err)
            }
        })
		so.On("disconnection", func() {
			log.Println("on disconnect")
		})
        log.Println("Rooms: ", so.Rooms())
	})
	server.On("error", func(so socketio.Socket, err error) {
		log.Println("error:", err)
	})
    go func() {
		for msg := range out {
            log.Println("Sending message: ", msg.Level, msg.Message)
			server.BroadcastTo("logs", msg.Level, msg.Message)
		}
	}()
	go func() {
		for err := range outErr {
            log.Println("Sending error: ", err)
            server.BroadcastTo("consumer_error", err.Error(), err)
		}
	}()
	http.Handle("/socket.io/", server)
    http.Handle("/", http.FileServer(http.Dir("./assets")))
	log.Println("Serving at localhost:5000...")
	log.Fatal(http.ListenAndServe(":5000", nil))
}

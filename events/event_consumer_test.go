package events

import (
	"sync"
	"testing"
)

func TestEventConsumer(t *testing.T) {
	offsetType = "newest"
	c, err := NewEventConsumer()
	if err != nil {
		t.Fatal("failed to create consumer: ", err)
	}
	err = doPublishLogEventTest()
	if err != nil {
		t.Fatal("publisher test failed: ", err)
	}
	out, outErr := c.Consume()
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		for err := range outErr {
			t.Fatal("Received error when consuming: ", err)
		}
	}()
	for msg := range out {
		t.Log("Received message from kafka: ", msg.Level, msg.Logger, msg.Line, msg.Message, msg.Date)
		c.Close()
		break
	}
}

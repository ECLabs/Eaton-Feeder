package events

import (
	"testing"
)

func TestPublishLogEvent(t *testing.T) {
	err := doPublishLogEventTest()
	if err != nil {
		t.Fatal("publisher test failed: ", err)
	}
}

func doPublishLogEventTest() error {
	p, err := NewEventPublisher()
	if err != nil {
		return err
	}
	p.PublishInfo("This is informational")
	p.PublishError("This is an error")
	p.PublishDebug("This is debug")
	p.PublishTrace("This is trace")
	err = p.Close()
	if err != nil {
		return err
	}
	return nil
}

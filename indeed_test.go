package main

import (
	"testing"
)

func TestIndeedClientPipeline(t *testing.T) {
	i := new(IndeedClient)
	k, err := NewKafkaProducer()
	if err != nil {
		t.Fatal("failed to create new kafka producer: ", err)
	}
	offsetType = "newest"
	c, err := NewKafkaConsumer()
	if err != nil {
		t.Fatal("failed to create new kafka consumer: ", err)
	}

	go func() {
		consumerErrorChannel := c.ConsumeMessages()
		for err := range consumerErrorChannel {
			t.Log("failed to consume msg: ", err)
		}
	}()

	errChannel, jobResultChannel := i.GetResults()
	kafkaErrChannel, kafkaDoneChannel := k.SendMessages(jobResultChannel)
	go func() {
		for err := range errChannel {
			t.Fatal("ERROR: ", err)
		}
	}()

	go func() {
		for err := range kafkaErrChannel {
			t.Fatal("Kafka ERROR: ", err)
		}
	}()

	go func() {
		for jobResult := range jobResultChannel {
			t.Log("Received JobResult: ", jobResult.JobKey)
			if jobResult.IsLast() {
				t.Log("and that was the last job result!")
			}
		}
	}()

	for kafkaDone := range kafkaDoneChannel {
		t.Log("finished sending to kafka!", kafkaDone)
	}
}

package main

import (
	"testing"
    eatonevents "github.com/ECLabs/Eaton-Feeder/events"
    eatonconfig "github.com/ECLabs/Eaton-Feeder/config"
)

func TestIndeedClientPipeline(t *testing.T) {
    eatonconfig.KafkaLoggerTopic = "logs-test"
    err := eatonevents.Init()
    if err != nil {
        t.Fatal("failed to create event publisher: ", err)
    }
	i := new(IndeedClient)
	scraper := new(IndeedScraper)
	k, err := NewKafkaProducer()
	if err != nil {
		t.Fatal("failed to create new kafka producer: ", err)
	}
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
	scraperErrChannel, scraperJobResultChannel := scraper.GetFullJobSummary(jobResultChannel)
	kafkaErrChannel, kafkaDoneChannel := k.SendMessages(scraperJobResultChannel)
	go func() {
        for err := range errChannel {
            t.Log("error thrown while polling indeed api: ", err)
		}
	}()

	go func() {
		for err := range kafkaErrChannel {
			t.Fatal("Kafka ERROR: ", err)
		}
	}()

	go func() {
		for jobResult := range scraperJobResultChannel {
			t.Log("Received JobResult: ", jobResult.JobKey)
			if jobResult.IsLast() {
				t.Log("and that was the last job result!")
			}
		}
	}()

	go func() {
		for err := range scraperErrChannel {
			t.Fatal("scraper error: ", err)
		}
	}()

	go func() {
		for jobResult := range jobResultChannel {
			t.Log("jobResult: ", jobResult)
		}
	}()

	for kafkaDone := range kafkaDoneChannel {
		t.Log("finished sending to kafka!", kafkaDone)
	}
}

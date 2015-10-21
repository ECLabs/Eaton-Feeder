package main

import (
	"encoding/xml"
	"github.com/Shopify/sarama"
    "github.com/ECLabs/Eaton-Feeder/ipresolver"
    "github.com/ECLabs/Eaton-Feeder/mapping"
    eatonconfig "github.com/ECLabs/Eaton-Feeder/config"
    eatonevents "github.com/ECLabs/Eaton-Feeder/events"
    "fmt"
)

type IndeedKafkaProducer struct {
	producer sarama.AsyncProducer
}

func NewKafkaProducer() (*IndeedKafkaProducer, error) {
	config := sarama.NewConfig()
	config.ClientID = ipresolver.GetLocalAddr()
	config.Producer.RequiredAcks = sarama.WaitForLocal
	config.Producer.Compression = sarama.CompressionNone
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true
	config.Producer.Partitioner = sarama.NewHashPartitioner
	asyncProducer, err := sarama.NewAsyncProducer(eatonconfig.KafkaServers, config)
	if err != nil {
		return nil, err
	}
	go func() {
		for msg := range asyncProducer.Successes() {
			eatonevents.Info(fmt.Sprintf("Successfully sent message to topic %s with key %s", msg.Topic, msg.Key))
		}
	}()
	go func() {
		for err := range asyncProducer.Errors() {
			eatonevents.Error("Failed to send message due to error: ", err)
		}
	}()
	return &IndeedKafkaProducer{
		producer: asyncProducer,
	}, nil
}

func (i *IndeedKafkaProducer) Close() error {
	return i.producer.Close()
}

func (i *IndeedKafkaProducer) SendMessages(jobResultChannel <-chan mapping.JobResult) (<-chan error, <-chan int) {
	errorChannel := make(chan error)
	kafkaDoneChannel := make(chan int)
	go func() {
		defer close(errorChannel)
		defer close(kafkaDoneChannel)
		defer i.Close()
		for jobResult := range jobResultChannel {
			if jobResult.IsLast() {
				eatonevents.Debug("received last jobResult. returning from function and signaling that the job is complete.")
				kafkaDoneChannel <- 0
				return
			}
			bytes, err := xml.Marshal(jobResult)
			if err != nil {
				errorChannel <- err
				continue
			}
            
            eatonevents.Debug(fmt.Sprintf("Sending JobResult JobKey: %s", jobResult.JobKey))
			i.producer.Input() <- &sarama.ProducerMessage{
				Topic: eatonconfig.KafkaTopic,
				Value: sarama.ByteEncoder(bytes),
				Key:   sarama.StringEncoder(jobResult.JobKey),
			}
		}
	}()
	return errorChannel, kafkaDoneChannel
}

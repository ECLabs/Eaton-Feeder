package main

import (
	"encoding/xml"
	"github.com/Shopify/sarama"
	"log"
	"os"
	"strings"
)

var (
	kafkaServers = strings.Split(os.Getenv("KAFKA_SERVERS"), ",")
	kafkaTopic   = os.Getenv("KAFKA_TOPIC")
)

type IndeedKafkaProducer struct {
	producer sarama.AsyncProducer
}

func NewKafkaProducer() (*IndeedKafkaProducer, error) {
	config := sarama.NewConfig()
	config.ClientID = GetLocalAddr()
	config.Producer.RequiredAcks = sarama.WaitForLocal
	config.Producer.Compression = sarama.CompressionNone
	config.Producer.Return.Successes = true
	config.Producer.Return.Errors = true
	config.Producer.Partitioner = sarama.NewHashPartitioner
	asyncProducer, err := sarama.NewAsyncProducer(kafkaServers, config)
	if err != nil {
		return nil, err
	}
	go func() {
		for msg := range asyncProducer.Successes() {
			log.Println("Successfully sent message to topic ", msg.Topic, " with key ", msg.Key)
		}
	}()
	go func() {
		for err := range asyncProducer.Errors() {
			log.Println("Failed to send message due to error: ", err)
		}
	}()
	return &IndeedKafkaProducer{
		producer: asyncProducer,
	}, nil
}

func (i *IndeedKafkaProducer) Close() error {
	return i.producer.Close()
}

func (i *IndeedKafkaProducer) SendMessages(jobResultChannel <-chan JobResult) (<-chan error, <-chan int) {
	errorChannel := make(chan error)
	kafkaDoneChannel := make(chan int)
	go func() {
		defer close(errorChannel)
		defer close(kafkaDoneChannel)
		defer i.Close()
		for jobResult := range jobResultChannel {
			if jobResult.IsLast() {
				if Debug {
					log.Println("received last jobResult. returning from function.")
				}
				kafkaDoneChannel <- 0
				return
			}
			bytes, err := xml.Marshal(jobResult)
			if err != nil {
				errorChannel <- err
				continue
			}
			i.producer.Input() <- &sarama.ProducerMessage{
				Topic: kafkaTopic,
				Value: sarama.ByteEncoder(bytes),
				Key:   sarama.StringEncoder(jobResult.JobKey),
			}
		}
	}()
	return errorChannel, kafkaDoneChannel
}

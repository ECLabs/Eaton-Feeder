package events

import (
	"encoding/xml"
	eatonconfig "github.com/ECLabs/Eaton-Feeder/config"
	eatonconsumer "github.com/ECLabs/Eaton-Feeder/consumer"
	"github.com/ECLabs/Eaton-Feeder/mapping"
	"github.com/Shopify/sarama"
	"sync"
)

type EventConsumer struct {
	consumer           sarama.Consumer
	partitionConsumers []sarama.PartitionConsumer
}

func (e *EventConsumer) Close() error {
	err := e.consumer.Close()
	if err != nil {
		return err
	}
	for _, p := range e.partitionConsumers {
		err = p.Close()
		if err != nil {
			return err
		}
	}
	return nil
}

func NewEventConsumer() (*EventConsumer, error) {
	consumer, partitionConsumers, err := eatonconsumer.NewSaramaConsumers(eatonconfig.KafkaServers, eatonconfig.KafkaLoggerTopic, eatonconfig.OffsetType)
	if err != nil {
		return nil, err
	}
	return &EventConsumer{
		consumer:           *consumer,
		partitionConsumers: *partitionConsumers,
	}, nil
}

func (e *EventConsumer) Consume() (<-chan mapping.ApplicationEvent, <-chan error) {
	out := make(chan mapping.ApplicationEvent)
	outErr := make(chan error)
	go func() {
		var wg sync.WaitGroup
		for _, partitionConsumer := range e.partitionConsumers {
			wg.Add(1)
			go func(partitionConsumer sarama.PartitionConsumer) {
				for msg := range partitionConsumer.Messages() {
					event := new(mapping.ApplicationEvent)
					err := xml.Unmarshal(msg.Value, event)
					if err != nil {
						outErr <- err
						continue
					}
					out <- *event
				}
				wg.Done()
			}(partitionConsumer)
			wg.Add(1)
			go func(partitionConsumer sarama.PartitionConsumer) {
				for err := range partitionConsumer.Errors() {
					outErr <- err.Err
				}
				wg.Done()
			}(partitionConsumer)
		}
		wg.Wait()
	}()
	return out, outErr
}

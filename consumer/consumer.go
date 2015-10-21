package consumer

import (
    "log"
    "github.com/ECLabs/Eaton-Feeder/ipresolver"
    eatonconfig "github.com/ECLabs/Eaton-Feeder/config"
    "github.com/Shopify/sarama"
    "errors"
)

    
func NewSaramaConsumers(servers []string, topic, offsetType string) (*sarama.Consumer, *[]sarama.PartitionConsumer, error) {
	config := sarama.NewConfig()
	config.ClientID = ipresolver.GetLocalAddr()
	config.Consumer.Return.Errors = true
	consumer, err := sarama.NewConsumer(servers, config)
	if err != nil {
		return nil, nil, err
	}
	partitions, err := consumer.Partitions(topic)
	if err != nil {
		return nil, nil, err
	}
	if eatonconfig.IsDebug() {
		log.Println("Returned Partitions for topic: ", topic, partitions)
	}
	if len(partitions) == 0 {
		return nil, nil, errors.New("no partitions returned to consume!")
	}
	partitionConsumers := make([]sarama.PartitionConsumer, len(partitions), len(partitions))
    chosenOffset := sarama.OffsetOldest
	switch offsetType {
	case "oldest":
		chosenOffset = sarama.OffsetOldest
		break
	case "newest":
		chosenOffset = sarama.OffsetNewest
		break
	default:
		log.Fatal("unknown offsetType provided: ", offsetType)
	}
	for index, partition := range partitions {
		if eatonconfig.IsDebug() {
			log.Println("Creating partition consumer for partition: ", partition, " with offset: ", chosenOffset)
		}
		partitionConsumer, err := consumer.ConsumePartition(topic, partition, chosenOffset)
		if eatonconfig.IsDebug() {
			log.Println("Created partition consumer: ", consumer)
		}
		if err != nil {
			return nil, nil, err
		}
		if partitionConsumer == nil {
			return nil, nil, errors.New("nil consumer returned!")
		}
		partitionConsumers[index] = partitionConsumer
	}
	return &consumer, &partitionConsumers, nil
}
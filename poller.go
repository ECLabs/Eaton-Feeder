package main

import (
	"bytes"
	"encoding/xml"
	"errors"
	"github.com/Shopify/sarama"
	"github.com/google/go-querystring/query"
	"io/ioutil"
	"log"
	"net/http"
	"strings"
)

type IndeedPoller struct {
	//Indeed API Parameters
	Publisher string `url:"publisher"`
	Version   string `url:"v"`
	Format    string `url:"format"`
	Query     string `url:"q"`
	Location  string `url:"l"`
	Sort      string `url:"sort"`
	Radius    int    `url:"radius"`
	SiteType  string `url:"st"`
	JobType   string `url:"jt"`
	Start     int    `url:"start"`
	Limit     int    `url:"limit"`
	FromAge   int    `url:"fromage,omitempty"`
	HighLight bool   `url:"highlight,int"`
	Filter    bool   `url:"filter,int"`
	LatLong   bool   `url:"latlong,int"`
	Country   string `url:"co"`
	Channel   string `url:"chnl"`
	UserIP    string `url:"userip"`
	UserAgent string `url:"useragent"`

	//Poller Behavior Configs
	//defines if the poller will pull from the indeed API
	//then send messages to kafka, or will poll the kafka
	//servers for messages to send to S3
	Consume            bool                       `url:"-"`
	BaseUrl            string                     `url:"-"`
	Interval           int                        `url:"-"`
	url                string                     `url:"-"`
	Producer           sarama.AsyncProducer       `url:"-"`
	Consumer           sarama.Consumer            `url:"-"`
	KafkaAddresses     string                     `url:"-"`
	addrs              []string                   `url:"-"`
	KafkaTopic         string                     `url:"-"`
	partitionConsumers []sarama.PartitionConsumer `url:"-"`
}

type HandleProducerMessage func(<-chan *sarama.ProducerMessage)
type HandleProducerError func(<-chan *sarama.ProducerError)

type HandleConsumerMessage func(<-chan *sarama.ConsumerMessage)
type HandleConsumerError func(<-chan *sarama.ConsumerError)

func (i *IndeedPoller) IsConsumer() bool {
	return i.Consume
}

func (i *IndeedPoller) IsProducer() bool {
	return !i.IsConsumer()
}

var DefaultHandleProducerMessage = func(successChannel <-chan *sarama.ProducerMessage) {
	for msg := range successChannel {
		log.Println("Successfully sent message to kafka: ", msg)
	}
}

var DefaultHandleProducerErrorMessage = func(errorChannel <-chan *sarama.ProducerError) {
	for err := range errorChannel {
		log.Println("Failed send message to kafka:", err)
	}
}

var DefaultHandleConsumerMessage = func(msgChannel <-chan *sarama.ConsumerMessage) {
    log.Println("Waiting for incoming messages...")
	for msg := range msgChannel {
		log.Println("Received Message: ", msg)
	}
}

var DefaultHandleConsumerError = func(errorChannel <-chan *sarama.ConsumerError) {
    log.Println("Waiting for incoming consumer errors...")
	for err := range errorChannel {
		log.Println("Failed consume from kafka:", err)
	}
}

//Initializes the producer that will
//send messages to the kafka servers
//with the passed in functions
//TODO: Create wrapper channels for success/error channels to reduce parameter count.
func (i *IndeedPoller) InitWithFunctions(handleProducerMessage HandleProducerMessage, handleProducerError HandleProducerError, handleConsumerMessage HandleConsumerMessage, handleConsumerError HandleConsumerError) error {
	config := sarama.NewConfig()
	config.ClientID = GetLocalAddr()
	if i.IsProducer() {
		config.Producer.RequiredAcks = sarama.WaitForLocal
		config.Producer.Compression = sarama.CompressionNone
		config.Producer.Return.Successes = true
		config.Producer.Return.Errors = true

		//partitions data based on the date that the job was posted
		config.Producer.Partitioner = sarama.NewHashPartitioner
		asyncProducer, err := sarama.NewAsyncProducer(i.addrs, config)
		if err != nil {
			return err
		}
		go handleProducerMessage(asyncProducer.Successes())
		go handleProducerError(asyncProducer.Errors())
		i.Producer = asyncProducer
	}
	if i.IsConsumer() {
		config.Consumer.Return.Errors = true
		consumer, err := sarama.NewConsumer(i.addrs, config)
		if err != nil {
			return err
		}
		i.Consumer = consumer
        log.Println("Finding topics...")
		topics, err := i.Consumer.Topics()
        if err != nil {
            return err
        }
        log.Println("Found topics: ", topics)
        partitions, err := i.Consumer.Partitions(i.KafkaTopic)
		if err != nil {
			return err
		}
		log.Println("Returned Partitions for topic: ", i.KafkaTopic, partitions)
		if len(partitions) == 0 {
			return errors.New("no partitions returned to consume!")
		}
        
		i.partitionConsumers = make([]sarama.PartitionConsumer, len(partitions), len(partitions))
		for index := range partitions {
			partition := partitions[index]
            log.Println("Creating partition consumer for partition: ", partition, " with offset: ", sarama.OffsetOldest)
			partitionConsumer, err := i.Consumer.ConsumePartition(i.KafkaTopic, partition, sarama.OffsetOldest)
            log.Println("Created partition consumer: ", consumer)
			if err != nil {
				return err
			}
            if partitionConsumer == nil {
                return errors.New("nil consumer returned!")
            }
			go handleConsumerMessage(partitionConsumer.Messages())
			go handleConsumerError(partitionConsumer.Errors())
			i.partitionConsumers[index] = partitionConsumer
		}
	}
	return nil
}

func (i *IndeedPoller) InitWithConsumerHandlerFunctions(handleConsumerMessage HandleConsumerMessage, handleConsumerError HandleConsumerError) error {
	return i.InitWithFunctions(DefaultHandleProducerMessage, DefaultHandleProducerErrorMessage, handleConsumerMessage, handleConsumerError)
}

func (i *IndeedPoller) InitWithProducerHandlerFunctions(handleProducerMessage HandleProducerMessage, handleProducerError HandleProducerError) error {
	return i.InitWithFunctions(handleProducerMessage, handleProducerError, DefaultHandleConsumerMessage, DefaultHandleConsumerError)
}

func (i *IndeedPoller) Init() error {
	return i.InitWithFunctions(DefaultHandleProducerMessage, DefaultHandleProducerErrorMessage, DefaultHandleConsumerMessage, DefaultHandleConsumerError)
}

func (i *IndeedPoller) Validate() error {
	if i.Publisher == "" {
		return errors.New("publisher is required!")
	}
	if i.Format != "xml" {
		return errors.New("xml is the only supported format right now!")
	}
	i.addrs = strings.Split(i.KafkaAddresses, ",")
	if len(i.addrs) == 0 {
		return errors.New("no kafka servers specified!")
	}
	return nil
}

func (i *IndeedPoller) GetUrl() string {
	if i.url != "" {
		return i.url
	}
	values, err := query.Values(i)
	if err != nil {
		log.Fatal("falied to parse struct: ", i, err)
	}
	buffer := new(bytes.Buffer)
	buffer.WriteString(i.BaseUrl)
	buffer.WriteString(values.Encode())
	i.url = buffer.String()
	log.Println("Full url: ", i.url)
	return i.url
}

func (i *IndeedPoller) GetMostRecentResult() (*ApiSearchResult, error) {
	url := i.GetUrl()
	resp, err := http.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	response := new(ApiSearchResult)
	log.Println("Body: ", string(body))
	err = xml.Unmarshal(body, response)
	if err != nil {
		return nil, err
	}
	return response, nil
}

func (i *IndeedPoller) SendResultToKafka(result *ApiSearchResult) {
	list := result.Results.JobResultList
	for index := range list {
		job := list[index]
		bytes, err := xml.Marshal(job)
		if err != nil {
			log.Println("Unable to marshal message to send: ", err)
			continue
		}
		i.Producer.Input() <- &sarama.ProducerMessage{
			Topic: i.KafkaTopic,
			Value: sarama.ByteEncoder(bytes),
			Key:   sarama.StringEncoder(job.GetDateString()),
		}
	}
}

func (i *IndeedPoller) ProduceMessages() error {
	if !i.IsProducer() {
		return errors.New("poller is not configured to be a producer!")
	}
	result, err := i.GetMostRecentResult()
	if err != nil {
		return err
	}
	i.SendResultToKafka(result)
	return nil
}

func (i *IndeedPoller) ConsumeMessages() error {
	return errors.New("not implemented!")
}

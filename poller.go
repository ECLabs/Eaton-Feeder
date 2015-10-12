package main

import (
	"bytes"
	"encoding/xml"
	"errors"
	"github.com/Shopify/sarama"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/dynamodb/dynamodbattribute"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/google/go-querystring/query"
	"io/ioutil"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"
)

var (
	dbSvc = dynamodb.New(nil)
	s3Svc = s3.New(nil)
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
	EndConsumeOnError  bool                       `url:"-"`
	BaseUrl            string                     `url:"-"`
	Interval           int                        `url:"-"`
	url                string                     `url:"-"`
	Producer           sarama.AsyncProducer       `url:"-"`
	Consumer           sarama.Consumer            `url:"-"`
	KafkaAddresses     string                     `url:"-"`
	addrs              []string                   `url:"-"`
	KafkaTopic         string                     `url:"-"`
	partitionConsumers []sarama.PartitionConsumer `url:"-"`
	Debug              bool                       `url:"-"`
	DynamoDbTableName  string                     `url:"-"`
	S3BucketName       string                     `url:"-"`
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
		if i.Debug {
			log.Println("Finding topics...")
		}
		topics, err := i.Consumer.Topics()
		if err != nil {
			return err
		}
		if len(topics) == 0 {
			return errors.New("no topics are available!")
		}
		if i.Debug {
			log.Println("Found topics: ", topics)
		}
		found := false
		for _, topic := range topics {
			found = strings.Compare(i.KafkaTopic, topic) == 0
			if found {
				break
			}
		}
		if !found {
			return errors.New("configured topic is not present in returned topics.")
		}
		partitions, err := i.Consumer.Partitions(i.KafkaTopic)
		if err != nil {
			return err
		}
		if i.Debug {
			log.Println("Returned Partitions for topic: ", i.KafkaTopic, partitions)
		}
		if len(partitions) == 0 {
			return errors.New("no partitions returned to consume!")
		}

		i.partitionConsumers = make([]sarama.PartitionConsumer, len(partitions), len(partitions))
		for index, partition := range partitions {
			if i.Debug {
				log.Println("Creating partition consumer for partition: ", partition, " with offset: ", sarama.OffsetOldest)
			}
			partitionConsumer, err := consumer.ConsumePartition(i.KafkaTopic, partition, sarama.OffsetOldest)
			if i.Debug {
				log.Println("Created partition consumer: ", consumer)
			}
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
	if i.KafkaTopic == "" {
		return errors.New("a kafka topic to produce/consume is required")
	}
	if i.Limit > MaxLimit {
		i.Limit = MaxLimit
	}
	if i.Limit < MinLimit {
		i.Limit = MinLimit
	}
	if i.Start < MinStart {
		i.Start = MinStart
	}
	return nil
}

func (i *IndeedPoller) GetUrl() string {
	if i.url != "" {
		return i.url
	}
	values, err := query.Values(i)
	if err != nil {
		log.Fatal("failed to parse struct of IndeedPoller: ", i, err)
	}
	buffer := new(bytes.Buffer)
	buffer.WriteString(i.BaseUrl)
	buffer.WriteString(values.Encode())
	i.url = buffer.String()
	if i.Debug {
		log.Println("Full url: ", i.url)
	}
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
	if i.Debug {
		log.Println("Body: ", string(body))
	}
	err = xml.Unmarshal(body, response)
	if err != nil {
		return nil, err
	}
	return response, nil
}

func (i *IndeedPoller) SendResultToKafka(result *ApiSearchResult) {
	list := result.Results.JobResultList
	for _, job := range list {
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
	if i.Interval < 0 {
		if i.Debug {
			log.Println("polling is disabled, will only execute once.")
		}
		err := i.doProduceMessages()
		if err != nil {
			return err
		}
		return nil
	}
	t := time.NewTicker(time.Duration(i.Interval) * time.Millisecond)
	for _ = range t.C {
		err := i.doProduceMessages()
        if err != nil {
			return err
		}
	}
    return nil
}

func (i *IndeedPoller) doProduceMessages() error {
	var wg sync.WaitGroup
	wg.Add(i.Limit)
	onSuccess := func(successChannel <-chan *sarama.ProducerMessage) {
		for success := range successChannel {
			log.Println("successfully sent message to kafka topic: ", success.Topic)
			wg.Done()
		}
	}
	onError := func(errChannel <-chan *sarama.ProducerError) {
		for err := range errChannel {
			log.Println("ERROR: failed to send message to kafka: ", err.Err.Error())
			wg.Done()
		}
	}
	err := i.InitWithProducerHandlerFunctions(onSuccess, onError)
	if err != nil {
		return err
	}
	result, err := i.GetMostRecentResult()
	if err != nil {
		return err
	}
	i.SendResultToKafka(result)
	wg.Wait()
	return nil
}

func (i *IndeedPoller) ConsumeMessages() error {
	if !i.IsConsumer() {
		return errors.New("poller is not configured to be a consumer!")
	}
	var wg sync.WaitGroup
	wg.Add(1)
	onMessage := func(msgChannel <-chan *sarama.ConsumerMessage) {
		if i.Debug {
			log.Println("Waiting for incoming messages...")
		}
		for msg := range msgChannel {
			if i.Debug {
				log.Println("Received Message: ", msg)
			}
			result := new(JobResult)
			err := xml.Unmarshal(msg.Value, result)
			if err != nil {
				log.Println("unable to marshal message into a JobResult: ", err)
				if i.EndConsumeOnError {
					wg.Done()
				}
                continue
			}
			//in order to get as much throughput as possible
			//the two aws api calls are executed in a different
			//goroutine than the one the channel is being read with.
            //TODO: Write and run tests confirming all works...
			go func(value []byte, result * JobResult) {
                _, err := s3Svc.PutObject(&s3.PutObjectInput{
					Bucket: aws.String(i.S3BucketName),
					Key:    aws.String(result.JobKey),
                    Body: bytes.NewReader(value),
				})
                
                if err != nil {
                    log.Println("failed to put job result into s3 bucket: ", i.S3BucketName, err)
                    if i.EndConsumeOnError {
                        wg.Done()
                    }
                    return
                }
                metadata := NewJobResultMetaData(result, i.S3BucketName, value)
                
				item, err := dynamodbattribute.ConvertToMap(metadata)
				if err != nil {
					log.Println("unable to convert JobResult to AttributeItem: ", err)
					if i.EndConsumeOnError {
						wg.Done()
					}
                    return
				}
				_, err = dbSvc.PutItem(&dynamodb.PutItemInput{
					Item:      item,
					TableName: aws.String(i.DynamoDbTableName),
				})
				if err != nil {
					log.Println("failed to save item to dynamodb: ", err)
					if i.EndConsumeOnError {
						wg.Done()
					}
                    return
				}

			}(msg.Value, result)

		}
	}
	onError := func(errorChannel <-chan *sarama.ConsumerError) {
		if i.Debug {
			log.Println("Waiting for incoming consumer errors...")
		}
		for err := range errorChannel {
			if i.EndConsumeOnError {
				wg.Done()
			}
			log.Println("Failed consume from kafka:", err)
		}
	}
	err := i.InitWithConsumerHandlerFunctions(onMessage, onError)
	defer func() {
		i.Consumer.Close()
	}()
	if err != nil {
		return err
	}
	wg.Wait()
	return nil
}

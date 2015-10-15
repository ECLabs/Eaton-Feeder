package main

import (
	"bytes"
	"encoding/xml"
	"errors"
	"fmt"
	"github.com/Shopify/sarama"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/s3"
	"log"
	"sync"
)

var (
	AWSRegion         = "us-west-2"
	envCreds          = credentials.NewEnvCredentials()
	config            = aws.NewConfig().WithCredentials(envCreds).WithRegion(AWSRegion)
	dbSvc             = dynamodb.New(config)
	s3Svc             = s3.New(config)
	retryCount        = 5
	chosenOffset      = sarama.OffsetOldest
	offsetType        = "oldest"
	awsWorkPoolSize   = 5
	S3BucketName      = "eaton-jobdescription-bucket"
	DynamoDBTableName = "Documents"
)

//kafkaServers,kafkaTopic defined in kafka_producer.go

type IndeedKafkaConsumer struct {
	consumer           sarama.Consumer
	partitionConsumers []sarama.PartitionConsumer
}

func NewKafkaConsumer() (*IndeedKafkaConsumer, error) {
	config := sarama.NewConfig()
	config.ClientID = GetLocalAddr()
	config.Consumer.Return.Errors = true
	consumer, err := sarama.NewConsumer(kafkaServers, config)
	if err != nil {
		return nil, err
	}
	partitions, err := consumer.Partitions(kafkaTopic)
	if err != nil {
		return nil, err
	}
	if Debug {
		log.Println("Returned Partitions for topic: ", kafkaTopic, partitions)
	}
	if len(partitions) == 0 {
		return nil, errors.New("no partitions returned to consume!")
	}
	partitionConsumers := make([]sarama.PartitionConsumer, len(partitions), len(partitions))
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
		if Debug {
			log.Println("Creating partition consumer for partition: ", partition, " with offset: ", chosenOffset)
		}
		partitionConsumer, err := consumer.ConsumePartition(kafkaTopic, partition, chosenOffset)
		if Debug {
			log.Println("Created partition consumer: ", consumer)
		}
		if err != nil {
			return nil, err
		}
		if partitionConsumer == nil {
			return nil, errors.New("nil consumer returned!")
		}
		partitionConsumers[index] = partitionConsumer
	}

	return &IndeedKafkaConsumer{
		consumer:           consumer,
		partitionConsumers: partitionConsumers,
	}, nil
}

func (i *IndeedKafkaConsumer) Close() error {
	err := i.consumer.Close()
	if err != nil {
		return err
	}
	for _, p := range i.partitionConsumers {
		err = p.Close()
		if err != nil {
			return err
		}
	}
	return nil
}

type AWSWork struct {
	jobResult JobResult
	msgValue  []byte
}

func (a *AWSWork) DoWork() error {
	var err error
	for i := 1; i < retryCount; i++ {
		_, err = s3Svc.PutObject(&s3.PutObjectInput{
			Bucket:      aws.String(S3BucketName),
			Key:         aws.String(a.jobResult.JobKey),
			Body:        bytes.NewReader(a.msgValue),
			ContentType: aws.String(fmt.Sprintf("application/%s", indeedConstants.IndeedResponseFormat)),
		})
		if err == nil {
			break
		}
	}
	if err != nil {
		return err
	}
	if a.jobResult.FullJobSummary != "" {
		log.Println("JobKey with FullSummary: ", a.jobResult.JobKey)
	}
	putItemInput := &dynamodb.PutItemInput{
		Item: map[string]*dynamodb.AttributeValue{
			//Minimum required fields as defined by EAT-3
			"DocumentID": {
				S: aws.String(a.jobResult.JobKey),
			},
			"Source": {
				S: aws.String("indeed"),
			},
			"Role": {
				S: aws.String("none"),
			},
			"Type": {
				S: aws.String("job description"),
			},
			"FileType": {
				S: aws.String(fmt.Sprintf("https://s3-%s.amazonaws.com/%s/%s", AWSRegion, S3BucketName, a.jobResult.JobKey)),
			},
			//extended metadata for the actual result.
			"CreateDate": {
				S: aws.String(a.jobResult.GetDateString()),
			},
			"JobTitle": {
				S: aws.String(a.jobResult.JobTitle),
			},
			"Company": {
				S: aws.String(a.jobResult.Company),
			},
			"City": {
				S: aws.String(a.jobResult.City),
			},
			"State": {
				S: aws.String(a.jobResult.State),
			},
			"Country": {
				S: aws.String(a.jobResult.Country),
			},
			"FormattedLocation": {
				S: aws.String(a.jobResult.FormattedLocation),
			},
			"ResultSource": {
				S: aws.String(a.jobResult.Source),
			},
			"Snippet": {
				S: aws.String(a.jobResult.Snippet),
			},
			"Latitude": {
				N: aws.String(fmt.Sprintf("%f", a.jobResult.Latitude)),
			},
			"Longitude": {
				N: aws.String(fmt.Sprintf("%f", a.jobResult.Longitude)),
			},
			"Sponsored": {
				BOOL: aws.Bool(a.jobResult.Sponsored),
			},
			"Expired": {
				BOOL: aws.Bool(a.jobResult.Expired),
			},
			"FormattedLocationFull": {
				S: aws.String(a.jobResult.FormattedLocationFull),
			},
			"FormattedRelativeTime": {
				S: aws.String(a.jobResult.FormattedRelativeTime),
			},
			"FullJobSummary": {
				S: aws.String(a.jobResult.FullJobSummary),
			},
		},
		TableName: aws.String(DynamoDBTableName),
	}
	for i := 1; i < retryCount; i++ {
		_, err = dbSvc.PutItem(putItemInput)
		if err == nil {
			break
		}
	}
	if err != nil {
		return err
	}
	log.Println("Successfully stored jobkey ", a.jobResult.JobKey, " in table ", DynamoDBTableName, " and in bucket ", S3BucketName)
	return nil
}

func (i *IndeedKafkaConsumer) ConsumeMessages() <-chan error {
	errChannel := make(chan error)
	go func() {
		defer close(errChannel)
		workChannel := make(chan AWSWork)
		defer close(workChannel)
		for j := 0; j < awsWorkPoolSize; j++ {
			go func() {
				for w := range workChannel {
					err := w.DoWork()
					if err != nil {
						errChannel <- err
					}
				}
			}()
		}
		var wg sync.WaitGroup
		wg.Add(len(i.partitionConsumers) * 2)
		for _, partitionConsumer := range i.partitionConsumers {
			go func(partitionConsumer sarama.PartitionConsumer) {
				for msg := range partitionConsumer.Messages() {
					result := new(JobResult)
					err := xml.Unmarshal(msg.Value, result)
					if err != nil {
						errChannel <- err
						continue
					}
					workChannel <- AWSWork{
						jobResult: *result,
						msgValue:  msg.Value,
					}
				}
				wg.Done()
			}(partitionConsumer)
			go func(partitionConsumer sarama.PartitionConsumer) {
				for err := range partitionConsumer.Errors() {
					errChannel <- err.Err
				}
				wg.Done()
			}(partitionConsumer)
		}
		wg.Wait()
	}()
	return errChannel
}

package main

import (
	"bytes"
	"encoding/xml"
	"fmt"
	"github.com/Shopify/sarama"
    eatonconsumer "github.com/ECLabs/Eaton-Feeder/consumer"
    eatonevents "github.com/ECLabs/Eaton-Feeder/events"
    eatonconfig "github.com/ECLabs/Eaton-Feeder/config"
    "github.com/ECLabs/Eaton-Feeder/mapping"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/service/dynamodb"
	"github.com/aws/aws-sdk-go/service/s3"
	"sync"
)

var (
	AWSRegion         = "us-west-2"
	envCreds          = credentials.NewEnvCredentials()
	config            = aws.NewConfig().WithCredentials(envCreds).WithRegion(AWSRegion)
	dbSvc             = dynamodb.New(config)
	s3Svc             = s3.New(config)
	retryCount        = 5
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
	consumer, partitionConsumers, err := eatonconsumer.NewSaramaConsumers(eatonconfig.KafkaServers, eatonconfig.KafkaTopic, eatonconfig.OffsetType)
	if err != nil {
		return nil, err
	}
	return &IndeedKafkaConsumer{
		consumer:           *consumer,
		partitionConsumers: *partitionConsumers,
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
	jobResult mapping.JobResult
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
	attrItem := map[string]*dynamodb.AttributeValue{
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
	}
	if a.jobResult.FullJobSummary != "" {
		attrItem["FullJobSummary"] = &dynamodb.AttributeValue{
			S: aws.String(a.jobResult.FullJobSummary),
		}
	}
	putItemInput := &dynamodb.PutItemInput{
		Item:      attrItem,
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
    msg := fmt.Sprintf("Successfully stored jobkey %s in table %s and in bucket %s", a.jobResult.JobKey, DynamoDBTableName, S3BucketName)
    err = eatonevents.Info(msg)
    if err != nil {
        return err
    }
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
					result := new(mapping.JobResult)
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

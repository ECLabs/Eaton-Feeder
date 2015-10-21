package main

import (
	"flag"
	eatonconfig "github.com/ECLabs/Eaton-Feeder/config"
    eatonevents "github.com/ECLabs/Eaton-Feeder/events"
	"log"
	"os"
    "fmt"
)

var (
	logFile   string
	doConsume = false
	doProduce = false
)

func main() {
	flag.StringVar(&S3BucketName, "bucket", "eaton-jobdescription-bucket", "the bucket to store retrieved indeed api messages from.")
	flag.StringVar(&DynamoDBTableName, "table", "Documents", "the dynamodb table to store the indeed api messages.")
	flag.StringVar(&logFile, "log", "eaton-feeder.log", "the log file to write results to.")
	flag.StringVar(&AWSRegion, "region", "us-west-2", "the aws region to use when saving content to dynamodb and s3.")
	flag.StringVar(&eatonconfig.OffsetType, "offset", "oldest", "the offset to use. either \"oldest\" or \"newest\" ")
	flag.BoolVar(&doConsume, "consume", false, "set to true to consume messages from KAFKA_SERVERS and send them to S3/DynamoDB")
	flag.BoolVar(&doProduce, "produce", false, "set to true to pull from the indeed api and push messages to KAFKA_SERVERS.")
	flag.IntVar(&interval, "interval", -1, "the time between polls of the indeed api in millis. anything equal to or below 0 disables the interval function.")
	flag.IntVar(&awsWorkPoolSize, "awsWorkPoolSize", 5, "the number of concurrent requests allowed when storing information in S3/DynamoDB")
	flag.Parse()

	log.Println("Using the following: ")
	log.Println("\tbucket:\t", S3BucketName)
	log.Println("\ttable:\t", DynamoDBTableName)
	log.Println("\tlogFile:\t", logFile)
	log.Println("\tregion:\t", AWSRegion)
	log.Println("\toffset:\t", eatonconfig.OffsetType)
	log.Println("\tconsume:\t", doConsume)
	log.Println("\tproduce:\t", doProduce)
	log.Println("\tinterval:\t", interval)
	log.Println("\tawsWorkPoolSize\t", awsWorkPoolSize)

	file, err := os.OpenFile(logFile, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		log.Fatal("ERROR - failed to create log file: ", err)
	}

	defer file.Close()
	log.SetOutput(file)

	if !doProduce && !doConsume {
		flag.PrintDefaults()
		return
	}

	if doProduce && doConsume {
		log.Fatal("ERROR - Cannot both produce and consume in a single application!")
	}

    err = eatonevents.Init()
    if err != nil {
        log.Fatal("ERROR - failed to create event publisher!")
    }
    
	var kafkaConsumer *IndeedKafkaConsumer
	var kafkaProducer *IndeedKafkaProducer

	if doProduce {
		if eatonconfig.IsDebug() {
			log.Println("Creating new IndeedKafkaProducer.")
		}
		indeedClient := new(IndeedClient)
		indeedScraper := new(IndeedScraper)
		kafkaProducer, err = NewKafkaProducer()
		if err != nil {
			eatonevents.Error("failed to create new kafka producer: ", err)
            os.Exit(1)
		}
		errChannel, jobResultChannel := indeedClient.GetResults()
		scraperErrChannel, scraperJobResultChannel := indeedScraper.GetFullJobSummary(jobResultChannel)
		kafkaErrChannel, kafkaDoneChannel := kafkaProducer.SendMessages(scraperJobResultChannel)

		go func() {
            eatonevents.Debug("Waiting for messages from the indeedClient error channel...")
            for err := range errChannel {
                eatonevents.Error("IndeedClient: ", err)
            }
            eatonevents.Debug("Finished waiting on messages from the indeedClient error channel.")
		}()
		go func() {
			eatonevents.Debug("Waiting on errors from the IndeedKafkaProducer error channel...")
			for err := range kafkaErrChannel {
				eatonevents.Error("IndeedKafkaProducer: ", err)
			}
			eatonevents.Debug("Finished waiting on messages from the IndeedKafkaProducer error channel.")
		}()
		go func() {
			eatonevents.Debug("Waiting on errors from the IndeedScraper error channel...")
			for err := range scraperErrChannel {
				eatonevents.Error("IndeedScraper: ", err)
			}
			eatonevents.Debug("Finshed waiting on messages from the indeed scraper error channel.")
		}()
		for done := range kafkaDoneChannel {
			eatonevents.Debug(fmt.Sprintf("completed sending messages to kafka (signal value: %d)", done))
			if kafkaConsumer != nil {
				kafkaConsumer.Close()
			}
		}
		return
	}

	if doConsume {
		eatonevents.Debug("Creating new IndeedKafkaConsumer.")
		kafkaConsumer, err = NewKafkaConsumer()
		if err != nil {
			eatonevents.Error("failed to create new kafka consumer: ", err)
            os.Exit(1)
		}
		errChannel := kafkaConsumer.ConsumeMessages()
		for err := range errChannel {
			eatonevents.Error("IndeedKafkaConsumer: ", err)
		}
	}
	
    eatonevents.Info("Main function has completed.  Exiting program.")
}
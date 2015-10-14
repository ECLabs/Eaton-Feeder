package main

import (
	"flag"
	"log"
	"net"
	"os"
	"sync"
)

var (
	logFile   string
	Debug     = false
	doConsume = false
	doProduce = false
	MyIP      string
)

func main() {
	flag.StringVar(&S3BucketName, "bucket", "eaton-jobdescription-bucket", "the bucket to store retrieved indeed api messages from.")
	flag.StringVar(&DynamoDBTableName, "table", "Documents", "the dynamodb table to store the indeed api messages.")
	flag.StringVar(&logFile, "log", "eaton-feeder.log", "the log file to write results to.")
	flag.StringVar(&AWSRegion, "region", "us-west-2", "the aws region to use when saving content to dynamodb and s3.")
	flag.StringVar(&offsetType, "offset", "oldest", "the offset to use. either \"oldest\" or \"newest\" ")
	flag.BoolVar(&doConsume, "consume", false, "set to true to consume messages from KAFKA_SERVERS and send them to S3/DynamoDB")
	flag.BoolVar(&doProduce, "produce", false, "set to true to pull from the indeed api and push messages to KAFKA_SERVERS.")
	flag.IntVar(&interval, "interval", -1, "the time between polls of the indeed api in millis. anything equal to or below 0 disables the interval function.")
	flag.BoolVar(&Debug, "debug", false, "set to true if more output is needed for testing purposes.")
	flag.IntVar(&awsWorkPoolSize, "awsWorkPoolSize", 5, "the number of concurrent requests allowed when storing information in S3/DynamoDB")
	flag.IntVar(&requestPoolSize, "indeedRequestPoolSize", 10, "the number of concurrent requests allowed when pulling information from the indeed api.")
	flag.Parse()

	log.Println("Using the following: ")
	log.Println("\tbucket:\t", S3BucketName)
	log.Println("\ttable:\t", DynamoDBTableName)
	log.Println("\tlogFile:\t", logFile)
	log.Println("\tregion:\t", AWSRegion)
	log.Println("\toffset:\t", offsetType)
	log.Println("\tconsume:\t", doConsume)
	log.Println("\tproduce:\t", doProduce)
	log.Println("\tinterval:\t", interval)
	log.Println("\tdebug:\t", Debug)
	log.Println("\tawsWorkPoolSize\t", awsWorkPoolSize)
	log.Println("\tindeedRequestPoolSize\t", requestPoolSize)

	file, err := os.OpenFile(logFile, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		log.Fatal("failed to create log file: ", err)
	}

	defer file.Close()
	log.SetOutput(file)

	if !doProduce && !doConsume {
		flag.PrintDefaults()
		return
	}
	var wg sync.WaitGroup
	var kafkaConsumer *IndeedKafkaConsumer
	var kafkaProducer *IndeedKafkaProducer

	if doProduce {
		if Debug {
			log.Println("Creating new IndeedKafkaProducer.")
		}
		indeedClient := new(IndeedClient)
		kafkaProducer, err = NewKafkaProducer()
		if err != nil {
			log.Fatal("failed to create new kafka producer: ", err)
		}
		errChannel, jobResultChannel := indeedClient.GetResults()
		kafkaErrChannel, kafkaDoneChannel := kafkaProducer.SendMessages(jobResultChannel)

		wg.Add(1)
		go func() {
			if Debug {
				log.Println("Waiting for messages from the indeedClient error channel...")
			}
			for err := range errChannel {
				log.Println("ERROR - IndeedClient: ", err)
			}
			if Debug {
				log.Println("Finished waiting on messages from the indeedClient error channel.")
			}
			wg.Done()
		}()

		wg.Add(1)
		go func() {
			if Debug {
				log.Println("Waiting on errors from the IndeedKafkaProducer error channel...")
			}
			for err := range kafkaErrChannel {
				log.Println("ERROR - IndeedKafkaProducer: ", err)
			}
			if Debug {
				log.Println("Finished waiting on messages from the IndeedKafkaProducer error channel.")
			}
			wg.Done()
		}()

		wg.Add(1)
		go func() {
			if Debug {
				log.Println("Waiting on done message from the IndeedKafkProducer done signal channel...")
			}
			for done := range kafkaDoneChannel {
				if Debug {
					log.Println("DEBUG - IndeedKafkaProducer: completed sending messages to kafka (signal value: ", done, ")")
				}
				if kafkaConsumer != nil {
					kafkaConsumer.Close()
				}
			}
			if Debug {
				log.Println("Finished waiting on done message from IndeedKafkaProducer done signal channel.")
			}
			wg.Done()
		}()
	}

	if doConsume {
		if Debug {
			log.Println("Creating new IndeedKafkaConsumer.")
		}
		kafkaConsumer, err = NewKafkaConsumer()
		if err != nil {
			log.Fatal("failed to create new kafka consumer: ", err)
		}
		errChannel := kafkaConsumer.ConsumeMessages()
		wg.Add(1)
		go func() {
			if Debug {
				log.Println("Waiting on errors from the IndeedKafkaConsumer error channel...")
			}
			for err := range errChannel {
				log.Println("ERROR - IndeedKafkaConsumer: ", err)
			}
			if Debug {
				log.Println("Finished waiting on errors from the IndeedKafkaConsumer error channel.")
			}
			wg.Done()
		}()
	}
	if Debug {
		log.Println("Waiting for producers/consumers to complete so the main function can exit...")
	}
	wg.Wait()
	if Debug {
		log.Println("Main function has completed.  Exiting program.")
	}
}

func GetLocalAddr() string {
	if MyIP != "" {
		return MyIP
	}
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		log.Println(err)
		os.Exit(1)
	}
	for _, a := range addrs {
		if ipnet, ok := a.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				MyIP = ipnet.IP.String()
			}
		}
	}
	return MyIP
}

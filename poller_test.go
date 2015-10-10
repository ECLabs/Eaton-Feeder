package main

import (
	"net/url"
	"os"
	"testing"
	"github.com/Shopify/sarama"
)

func TestValidate(t *testing.T) {
	i := new(IndeedPoller)
	err := i.Validate()
	if err == nil {
		t.Fatal("publisher was not set, validate should fail!")
	}

	i.Publisher = "12345667"
	i.Format = "xml"
	i.KafkaAddresses = "localhost:9092"
	err = i.Validate()

	if err != nil {
		t.Fatal("validate should pass, publisher and format was set.")
	}
}

/*
More testing can be added for each value
set within the query string.  this seems
sufficient for now though.
*/
func TestGetUrl(t *testing.T) {
	i := new(IndeedPoller)
	i.Publisher = "1234567"
	i.BaseUrl = "http://api.indeed.com/api/?"
	i.SiteType = "jobsite"
	i.Limit = 50
	requestString := i.GetUrl()
	requestUrl, err := url.Parse(requestString)

	if err != nil {
		t.Fatal("failed to parse requestString", err)
	}

	values := requestUrl.Query()

	publishers := values["publisher"]

	if len(publishers) != 1 {
		t.Fatal("incorrect number of publishers were set!", publishers)
	}

	if publishers[0] != i.Publisher {
		t.Fatal("set publisher not existant in query string!", publishers[0], i.Publisher)
	}

	siteTypes := values["st"]

	if len(siteTypes) != 1 {
		t.Fatal("incorrect number of siteTypes set!", siteTypes)
	}

	if siteTypes[0] != i.SiteType {
		t.Fatal("set siteType does not match value in query string!", siteTypes[0], i.SiteType)
	}

	t.Log("Url: ", requestUrl)
}

//TODO: Max limit allowed is 25, not 50.
func TestGetMostRecentResult(t *testing.T) {
	i := getPoller(t)
	result, err := i.GetMostRecentResult()
	if err != nil {
		t.Fatal("failed to get result: ", err)
	}
	t.Log("Got result: ", result)
	list := result.Results.JobResultList
	if list == nil || len(list) != i.Limit {
		t.Fatal("no results in body!", list)
	}
	for index := range list {
		item := list[index]
		if item.JobTitle == "" {
			t.Fatal("jobtitle not mapped!")
		}
		if item.Company == "" {
			t.Fatal("company not mapped!")
		}
		if item.Date.Unix() == 0 {
			t.Fatal("date not mapped!")
		}
		t.Log("Date: ", item.Date.Unix())
	}
}

func getPoller(t * testing.T) *IndeedPoller {
	i := new(IndeedPoller)
	i.Publisher = os.Getenv("INDEED_PUBLISHER_ID")
	if i.Publisher == "" {
		t.Fatal("missing INDEED_PUBLISHER_ID os env variable")
	}
	i.Format = "xml"
	i.BaseUrl = "http://api.indeed.com/ads/apisearch?"
	i.Version = "2"
	i.Location = "22033"
	i.Sort = "relevance"
	i.Radius = 25
	i.JobType = "fulltime"
	i.Start = 0
	i.Limit = 25
	i.FromAge = 0
	i.HighLight = false
	i.Filter = true
	i.LatLong = true
	i.Country = "us"
	i.UserIP = GetLocalAddr()
	i.UserAgent = "Golang http client"
	i.KafkaAddresses = os.Getenv("KAFKA_SERVERS")
    i.KafkaTopic = "eaton-feeder-test"
	if i.KafkaAddresses == "" {
		t.Fatal("missing KAFKA_SERVERS os env variable")
	}
    err := i.Validate()
    if err != nil {
        t.Fatal("not a valid poller config: ", err)
    }
	return i
}

func TestSendMessageToKafka(t *testing.T) {
	i := getPoller(t)
	resultChannel := make(chan bool)
	successFn := func(successChannel <-chan *sarama.ProducerMessage) {
		for msg := range successChannel {
			t.Log("Successfully sent message to kafka: ", msg)
			resultChannel <- true
			break
		}
	}
	errorFn := func(errChannel <-chan *sarama.ProducerError) {
		for err := range errChannel {
			t.Log("Failed to send ", err)
			resultChannel <- false
			break
		}
	}
	defer func() {
		i.Producer.Close()
	}()
	err := i.InitWithProducerHandlerFunctions(successFn, errorFn)
	if err != nil {
		t.Fatal("failed to initialize kafka producers: ", err)
	}
	result, err := i.GetMostRecentResult()
	if err != nil {
		t.Fatal("failed to get result: ", err)
	}
	i.SendResultToKafka(result)
	success := <-resultChannel
	if !success {
		t.Fatal("failed to successfully send message!")
	}
}

func TestConsumeMessageFromKafka(t * testing.T){
    i := getPoller(t)
    i.Consume = true
    resultChannel := make(chan bool)
    onMsg := func(msgChannel <-chan *sarama.ConsumerMessage) {
        t.Log("Waiting for incoming messages...")
        for msg := range msgChannel {
            t.Log("Received Message: ", msg)
            resultChannel <- true
        }
    }
    onErr := func(errorChannel <-chan *sarama.ConsumerError) {
        t.Log("Waiting for incoming consumer errors...")
        for err := range errorChannel {
            t.Log("Failed consume from kafka:", err)
            resultChannel <- false
        }
    }
    i.InitWithConsumerHandlerFunctions(onMsg, onErr)
    defer func(){
        i.Consumer.Close()
        for index := range i.partitionConsumers {
            c := i.partitionConsumers[index]
            c.Close()
        }
    }()
    result := <- resultChannel
    if !result {
        t.Fatal("failed to get messages!")
    }
}

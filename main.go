package main

import (
	"flag"
	"log"
	"net"
	"os"
	"sync"
)

var (
	poller = new(IndeedPoller)
	MyIP   string
	wg     sync.WaitGroup
	//Max limit for the indeed api is 25
	MaxLimit = 25
	MinLimit = 1
	MinStart = 0
	logFile  string
)

func main() {
	flag.StringVar(&poller.BaseUrl, "baseUrl", "http://api.indeed.com/ads/apisearch?", "base url for api.indeed.com")
	flag.StringVar(&poller.Publisher, "publisher", "", "Publisher ID. This is assigned when you register as a publisher.")
	flag.StringVar(&poller.Version, "version", "2", "Which version of the API you wish to use. All publishers should be using version 2. Currently available versions are 1 and 2")
	flag.StringVar(&poller.Format, "format", "xml", "Which output format of the API you wish to use. The options are \"xml\" and \"json\". If omitted or invalid, the XML format is used.")
	flag.StringVar(&poller.Query, "query", "", "By default terms are ANDed. To see what is possible, use our advanced search page to perform a search and then check the url for the q value.")
	flag.StringVar(&poller.Location, "location", "", "Use a postal code or a \"city, state/province/region\" combination.")
	flag.StringVar(&poller.Sort, "sort", "relevance", "Sort by relevance or date.")
	flag.IntVar(&poller.Radius, "radius", 25, "Distance from search location (\"as the crow flies\").")
	flag.StringVar(&poller.SiteType, "siteType", "", "To show only jobs from job boards use \"jobsite\". For jobs from direct employer websites use \"employer\".")
	flag.StringVar(&poller.JobType, "jobType", "fulltime", "Allowed values: \"fulltime\", \"parttime\", \"contract\", \"internship\", \"temporary\".")
	flag.IntVar(&poller.Start, "start", MinStart, "Start results at this result number, beginning with 0.")
	flag.IntVar(&poller.Limit, "limit", MaxLimit, "Maximum number of results returned per query.")
	flag.IntVar(&poller.FromAge, "fromAge", 0, "Number of days back to search.")
	flag.BoolVar(&poller.HighLight, "highlight", false, "Setting this value to true will bold terms in the snippet that are also present in Query. Default is false.")
	flag.BoolVar(&poller.Filter, "filter", true, "Filter duplicate results. False turns off duplicate job filtering. Default is true.")
	flag.BoolVar(&poller.LatLong, "latLong", false, "If latLong=true, returns latitude and longitude information for each job result. Default is false.")
	flag.StringVar(&poller.Country, "country", "us", "Search within country specified.")
	flag.StringVar(&poller.Channel, "channel", "", "Channel Name: Group API requests to a specific channel")
	flag.StringVar(&poller.UserIP, "userIP", GetLocalAddr(), "The IP number of the end-user to whom the job results will be displayed.")
	flag.StringVar(&poller.UserAgent, "userAgent", "Golang http client", "The User-Agent (browser) of the end-user to whom the job results will be displayed. This can be obtained from the \"User-Agent\" HTTP request header from the end-user.")
	flag.IntVar(&poller.Interval, "interval", 1000, "interval in millis between each poll (less than 0 will only have it run once). This is ignored if in --consume=true")
	flag.StringVar(&poller.KafkaAddresses, "kafkaServers", "", "a comma delimited list of host:port values where kafka is running. (ex. 192.168.0.1:9092,192.168.0.2:9092")
	flag.StringVar(&poller.KafkaTopic, "kafkaTopic", "", "the topic to consume from or produce to.")
	flag.BoolVar(&poller.Consume, "consume", false, "sets this poller as a consumer (will post data to S3/DynamoDB instead of pulling from indeed API if this is set to true)")
	flag.BoolVar(&poller.Debug, "debug", false, "set logging level to debug.")
	flag.StringVar(&poller.S3BucketName, "bucket", "eaton-jobdescription-bucket", "the bucket to store retrieved indeed api messages from.")
	flag.StringVar(&poller.DynamoDbTableName, "table", "Documents", "the dynamodb table to store the indeed api messages.")
	flag.StringVar(&logFile, "log", "eaton-feeder.log", "the log file to write results to.")
	flag.StringVar(&AWSRegion, "region", "us-west-2", "the aws region to use when saving content to dynamodb and s3.")
	flag.StringVar(&offsetType, "offset", "oldest", "the offset to use. either \"oldest\" or \"newest\" ")
	flag.Parse()

	file, err := os.OpenFile(logFile, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		log.Fatal("failed to create log file: ", err)
	}

	defer file.Close()
	log.SetOutput(file)

	if flag.NFlag() == 0 {
		flag.PrintDefaults()
		return
	}

	err = poller.Validate()

	if err != nil {
		log.Fatal(err)
	}

	if poller.IsProducer() {
		err = poller.ProduceMessages()
	} else {
		err = poller.ConsumeMessages()
	}

	if err != nil {
		log.Fatal("failed to run poller: ", err)
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

package main

import (
	"flag"
	"log"
	"net"
	"os"
)

var (
	poller = new(IndeedPoller)
	MyIP   string
)

func println(args ...interface{}) {
	log.Println(args)
}

func main() {
	flag.StringVar(&poller.BaseUrl, "baseUrl", "http://api.indeed.com/ads/apisearch?", "base url for api.indeed.com")
	flag.StringVar(&poller.Publisher, "publisher", "", "Publisher ID. This is assigned when you register as a publisher.")
	flag.StringVar(&poller.Version, "version", "2", "Which version of the API you wish to use. All publishers should be using version 2. Currently available versions are 1 and 2")
	flag.StringVar(&poller.Format, "format", "xml", "Which output format of the API you wish to use. The options are \"xml\" and \"json\". If omitted or invalid, the XML format is used.")
	flag.StringVar(&poller.Query, "query", "", "By default terms are ANDed. To see what is possible, use our advanced search page to perform a search and then check the url for the q value.")
	flag.StringVar(&poller.Location, "location", "22033", "Use a postal code or a \"city, state/province/region\" combination.")
	flag.StringVar(&poller.Sort, "sort", "relevance", "Sort by relevance or date.")
	flag.IntVar(&poller.Radius, "radius", 25, "Distance from search location (\"as the crow flies\").")
	flag.StringVar(&poller.SiteType, "siteType", "", "To show only jobs from job boards use \"jobsite\". For jobs from direct employer websites use \"employer\".")
	flag.StringVar(&poller.JobType, "jobType", "fulltime", "Allowed values: \"fulltime\", \"parttime\", \"contract\", \"internship\", \"temporary\".")
	flag.IntVar(&poller.Start, "start", 0, "Start results at this result number, beginning with 0.")
	flag.IntVar(&poller.Limit, "limit", 50, "Maximum number of results returned per query.")
	flag.IntVar(&poller.FromAge, "fromAge", 0, "Number of days back to search.")
	flag.BoolVar(&poller.HighLight, "highlight", false, "Setting this value to true will bold terms in the snippet that are also present in Query. Default is false.")
	flag.BoolVar(&poller.Filter, "filter", true, "Filter duplicate results. False turns off duplicate job filtering. Default is true.")
	flag.BoolVar(&poller.LatLong, "latLong", false, "If latLong=true, returns latitude and longitude information for each job result. Default is false.")
	flag.StringVar(&poller.Country, "country", "us", "Search within country specified.")
	flag.StringVar(&poller.Channel, "channel", "", "Channel Name: Group API requests to a specific channel")
	flag.StringVar(&poller.UserIP, "userIP", GetLocalAddr(), "The IP number of the end-user to whom the job results will be displayed.")
	flag.StringVar(&poller.UserAgent, "userAgent", "Golang http client", "The User-Agent (browser) of the end-user to whom the job results will be displayed. This can be obtained from the \"User-Agent\" HTTP request header from the end-user.")
	flag.IntVar(&poller.Interval, "interval", 1000, "interval in millis between each poll (less than 0 will only have it run once)")
	flag.StringVar(&poller.OutputFile, "file", "output", "the output for the results to be exported to.  file type (json/xml) is appended by default.")
	flag.Parse()

	err := poller.Validate()

	if err != nil {
		println(err)
		os.Exit(1)
	}

	poller.Poll()
}

func GetLocalAddr() string {
	if MyIP != "" {
		return MyIP
	}
	addrs, err := net.InterfaceAddrs()
	if err != nil {
		println(err)
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

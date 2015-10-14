package main

import (
	"bytes"
	"encoding/xml"
	"fmt"
	"github.com/google/go-querystring/query"
	"io/ioutil"
	"log"
	"math"
	"net/http"
	"os"
	"sync"
	"time"
)

var (
	indeedConstants = &IndeedDefaults{
		IndeedUrl:            "http://api.indeed.com/ads/apisearch?",
		PublisherId:          os.Getenv("INDEED_PUBLISHER_ID"),
		IndeedResponseFormat: "xml",
		Version:              "2",
		FromAge:              0,
		HighLight:            false,
		Filter:               true,
		LatLong:              true,
		Country:              "us",
		Channel:              "",
		UserIP:               GetLocalAddr(),
		UserAgent:            "golang indeed client",
		Location:             "22033",
	}
	constantValues, queryError = query.Values(indeedConstants)
	encodedConstants           = constantValues.Encode()
	maxLimit                   = 25
	requestPoolSize            = 10
	interval                   = -1
)

type IndeedDefaults struct {
	IndeedUrl            string `url:"-"`
	PublisherId          string `url:"publisher"`
	IndeedResponseFormat string `url:"format"`
	Version              string `url:"v"`
	FromAge              int    `url:"fromage,omitempty"`
	HighLight            bool   `url:"highlight,int"`
	Filter               bool   `url:"filter,int"`
	LatLong              bool   `url:"latlong,int"`
	Country              string `url:"co"`
	Channel              string `url:"chnl"`
	UserIP               string `url:"userip"`
	UserAgent            string `url:"useragent"`
	Location             string `url:"l"`
}

type IndeedClient struct {
	//Intended for future use when
	//pulling for multiple locations
	Location string `url:"l"`
}

type Work struct {
	Start            int
	jobResultChannel chan JobResult
}

func (w *Work) getIndeedResults() error {
	apiSearchResult, err := w.getResults()
	if err != nil {
		return err
	}
	list := apiSearchResult.Results.JobResultList
	for _, jobResult := range list {
		w.jobResultChannel <- jobResult
	}
	return nil
}

func (w *Work) getResults() (*ApiSearchResult, error) {
	buffer := new(bytes.Buffer)
	buffer.WriteString(indeedConstants.IndeedUrl)
	buffer.WriteString(encodedConstants)
	buffer.WriteString("&start=")
	buffer.WriteString(fmt.Sprintf("%d", w.Start))
	buffer.WriteString("&limit=")
	buffer.WriteString(fmt.Sprintf("%d", maxLimit))
	myUrl := buffer.String()
	if Debug {
		log.Println("Getting url ", myUrl, " ", w.Start)
	}
	resp, err := http.Get(myUrl)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	apiSearchResult := new(ApiSearchResult)
	if Debug {
		log.Println("Body: ", string(body))
	}
	err = xml.Unmarshal(body, apiSearchResult)
	if err != nil {
		return nil, err
	}
	return apiSearchResult, nil
}

func (w *Work) GetTotalResultCount() (int, error) {
	r, err := w.getResults()
	if err != nil {
		return -1, err
	}
	return r.TotalResults, nil
}

func getTotalResults() (int, error) {
	w := Work{}
	w.Start = 0
	return w.GetTotalResultCount()
}

func (i *IndeedClient) GetResults() (<-chan error, <-chan JobResult) {
	if queryError != nil {
		log.Fatal("unable to encode default constants: ", queryError)
	}
	errChannel := make(chan error)
	jobResultChannel := make(chan JobResult)

	go func() {
		defer close(errChannel)
		defer close(jobResultChannel)
		workChannel := make(chan Work)
		defer close(workChannel)
		var wg sync.WaitGroup

		for i := 0; i < requestPoolSize; i++ {
			go func() {
				for work := range workChannel {
					work.getIndeedResults()
					wg.Done()
				}
			}()
		}
		chosenInterval := 1
		if interval > 0 {
			chosenInterval = interval
		}
		ticker := time.NewTicker(time.Duration(chosenInterval) * time.Millisecond)
		for _ = range ticker.C {
			totalResults, err := getTotalResults()
			if err != nil {
				errChannel <- err
				jobResultChannel <- NewLastJobResult()
				return
			}
			wg.Add(int(math.Ceil(float64(totalResults) / float64(maxLimit))))
			for start := 0; start < totalResults; start += maxLimit {
				workChannel <- Work{
					Start:            start,
					jobResultChannel: jobResultChannel,
				}
			}
			wg.Wait()
			if interval <= 0 {
				break
			}
		}
		jobResultChannel <- NewLastJobResult()
	}()

	return errChannel, jobResultChannel
}

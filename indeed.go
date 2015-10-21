package main

import (
	"bytes"
	"encoding/xml"
	"errors"
	"fmt"
	eatonevents "github.com/ECLabs/Eaton-Feeder/events"
	"github.com/ECLabs/Eaton-Feeder/ipresolver"
	"github.com/ECLabs/Eaton-Feeder/mapping"
	"github.com/google/go-querystring/query"
	"io/ioutil"
	"log"
	"net/http"
	"os"
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
		UserIP:               ipresolver.GetLocalAddr(),
		UserAgent:            "golang indeed client",
		Location:             "22033",
	}
	constantValues, queryError = query.Values(indeedConstants)
	encodedConstants           = constantValues.Encode()
	maxLimit                   = 25
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

func (i *IndeedClient) getIndeedResultPage(start int) (*mapping.ApiSearchResult, error) {
	buffer := new(bytes.Buffer)
	buffer.WriteString(indeedConstants.IndeedUrl)
	buffer.WriteString(encodedConstants)
	buffer.WriteString("&start=")
	buffer.WriteString(fmt.Sprintf("%d", start))
	buffer.WriteString("&limit=")
	buffer.WriteString(fmt.Sprintf("%d", maxLimit))
	myUrl := buffer.String()
    eatonevents.Debug(fmt.Sprintf("Getting url %s", myUrl))
	resp, err := http.Get(myUrl)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	apiSearchResult := new(mapping.ApiSearchResult)
	err = xml.Unmarshal(body, apiSearchResult)
	if err != nil {
		return nil, err
	}
	if apiSearchResult.Start != (start + 1) {
		return nil, errors.New(fmt.Sprintf("indeed search returned incorrect offset. expected %d but received %d", start + 1, apiSearchResult.Start))
	}
	return apiSearchResult, nil
}

func (i *IndeedClient) GetResults() (<-chan error, <-chan mapping.JobResult) {
	if queryError != nil {
		log.Fatal("unable to encode default constants: ", queryError)
	}
	errChannel := make(chan error)
	jobResultChannel := make(chan mapping.JobResult)

	go func() {
		defer func() {
			jobResultChannel <- mapping.NewLastJobResult()
			close(errChannel)
			close(jobResultChannel)
		}()
		chosenInterval := 1
		if interval > 0 {
			chosenInterval = interval
		}
		ticker := time.NewTicker(time.Duration(chosenInterval) * time.Millisecond)
		for _ = range ticker.C {
			totalResults := maxLimit
			for start := 0; start < totalResults; start += maxLimit {
				apiSearchResult, err := i.getIndeedResultPage(start)
				if err != nil {
					errChannel <- err
					return
				}
				totalResults = apiSearchResult.TotalResults
				list := apiSearchResult.Results.JobResultList
				for _, jobResult := range list {
					jobResultChannel <- jobResult
				}
			}
			if interval <= 0 {
				break
			}
		}
	}()
	return errChannel, jobResultChannel
}

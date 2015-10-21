package main

import (
	"errors"
	"fmt"
	"github.com/ECLabs/Eaton-Feeder/mapping"
	"github.com/PuerkitoBio/goquery"
	"net/url"
)

type IndeedScraper struct {
}

func (i *IndeedScraper) doGetFullJobSumary(jobResult mapping.JobResult) (*mapping.JobResult, error) {
	indeedUrl, err := url.Parse(jobResult.Url)
	if err != nil {
		return nil, err
	}
	if indeedUrl.Host != "www.indeed.com" {
		return nil, errors.New(fmt.Sprintf("unknown job summary host: %s", indeedUrl.Host))
	}

	doc, err := goquery.NewDocument(jobResult.Url)
	if err != nil {
		return nil, err
	}
	var findErr error
	fullSummary := ""
	doc.Find("span#job_summary").Each(func(i int, s *goquery.Selection) {
		fullSummary, findErr = s.Html()
	})
	if findErr != nil {
		return nil, findErr
	}
	if fullSummary == "" {
		return nil, errors.New(fmt.Sprintf("couldn't find full summary for jobKey: %s", jobResult.JobKey))
	}
	jobResult.FullJobSummary = fullSummary
	return &jobResult, nil
}

func (i *IndeedScraper) GetFullJobSummary(input <-chan mapping.JobResult) (<-chan error, <-chan mapping.JobResult) {
	errChannel := make(chan error)
	output := make(chan mapping.JobResult)
	go func() {
		defer func() {
			close(errChannel)
			close(output)
		}()
		for jobResult := range input {
			if jobResult.IsLast() {
				output <- jobResult
				return
			}
			jr, err := i.doGetFullJobSumary(jobResult)
			if err != nil {
				errChannel <- err
				continue
			}
			output <- *jr
		}

	}()
	return errChannel, output
}

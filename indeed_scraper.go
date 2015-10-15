package main

import (
	"errors"
	"fmt"
	"github.com/PuerkitoBio/goquery"
	"net/url"
	"sync"
)

var (
	indeedScrapePoolSize = 5
)

type IndeedScraper struct {
}

type indeedScraperWorker struct {
	jobResult JobResult
}

func (i *indeedScraperWorker) doGetFullJobSumary(jobResultChannel chan JobResult) error {
	indeedUrl, err := url.Parse(i.jobResult.Url)
	if err != nil {
		return err
	}
	if indeedUrl.Host != "www.indeed.com" {
		return errors.New(fmt.Sprintf("unknown job summary host: %s", indeedUrl.Host))
	}

	doc, err := goquery.NewDocument(i.jobResult.Url)
	if err != nil {
		return err
	}
	var findErr error
	fullSummary := ""
	doc.Find("span#job_summary").Each(func(i int, s *goquery.Selection) {
		fullSummary, findErr = s.Html()
	})
	if findErr != nil {
		return findErr
	}
	if fullSummary == "" {
		return errors.New(fmt.Sprintf("couldn't find full summary for jobKey: %s", i.jobResult.JobKey))
	}
	i.jobResult.FullJobSummary = fullSummary
	jobResultChannel <- i.jobResult
	return nil
}

func (i *IndeedScraper) GetFullJobSummary(input <-chan JobResult) (<-chan error, <-chan JobResult) {
	errChannel := make(chan error)
	output := make(chan JobResult)
	go func() {
		workChannel := make(chan indeedScraperWorker)
		var wg sync.WaitGroup
		defer func() {
			close(workChannel)
			wg.Wait()
			close(errChannel)
			close(output)
		}()
		for j := 0; j < indeedScrapePoolSize; j++ {
			wg.Add(1)
			go func() {
				for w := range workChannel {
					err := w.doGetFullJobSumary(output)
					if err != nil {
						errChannel <- err
					}
				}
				wg.Done()
			}()
		}
		for jobResult := range input {
			if jobResult.IsLast() {
				output <- jobResult
				return
			}
			workChannel <- indeedScraperWorker{
				jobResult: jobResult,
			}
		}
	}()
	return errChannel, output
}

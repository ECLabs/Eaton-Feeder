package main

import (
	"bytes"
	"errors"
	"github.com/google/go-querystring/query"
	"io/ioutil"
	"log"
	"net/http"
	"time"
    "fmt"
)

type IndeedPoller struct {
	Publisher string `url:"publisher"`
	Version   string `url:"v"`
	Format    string `url:"format"`
	Query     string `url:"q"`
	Location  string `url:"l"`
	Sort      string `url:"sort"`
	Radius    int    `url:"radius"`
	SiteType  string `url:"st"`
	JobType   string `url:"jt"`
	Start     int    `url:"start"`
	Limit     int    `url:"limit"`
	FromAge   int    `url:"fromage,omitempty"`
	HighLight bool   `url:"highlight,int"`
	Filter    bool   `url:"filter,int"`
	LatLong   bool   `url:"latlong,int"`
	Country   string `url:"co"`
	Channel   string `url:"chnl"`
	UserIP    string `url:"userip"`
	UserAgent string `url:"useragent"`

	BaseUrl    string `url:"-"`
	Interval   int    `url:"-"`
	url        string `url:"-"`
	OutputFile string `url:"-"`
}

func (i *IndeedPoller) Validate() error {
	if i.Publisher == "" {
		return errors.New("publisher is required!")
	}
	return nil
}

func (i *IndeedPoller) GetUrl() string {
	if i.url != "" {
		return i.url
	}
	values, err := query.Values(i)
	if err != nil {
		log.Fatal("falied to parse struct: ", i, err)
	}
	buffer := new(bytes.Buffer)
	buffer.WriteString(i.BaseUrl)
	buffer.WriteString(values.Encode())
	i.url = buffer.String()
	println("Full url: ", i.url)
	return i.url
}

func (i *IndeedPoller) DoPoll() {
	url := i.GetUrl()
	println("Starting poll: ", url)
	resp, err := http.Get(url)
	if err != nil {
		println("failed to get response from indeed: ", err)
		return
	}
	defer resp.Body.Close()
	body, err := ioutil.ReadAll(resp.Body)
    if err != nil {
        println("unable to read/write response body to file.", err)
        return
    }
	println("Finished poll: ", url, len(body))
    if err = i.WriteResultsToFile(body); err != nil {
        println("failed to write body to file: ", err)
    }
}

func (i * IndeedPoller) GetFileName() string{
    return fmt.Sprintf("%s-%d.%s", i.OutputFile, time.Now().Unix(), i.Format)
}

func (i *IndeedPoller) WriteResultsToFile(body []byte) error {
    return ioutil.WriteFile(i.GetFileName(), body, 0644)
}

func (i *IndeedPoller) Poll() {
	if i.Interval < 0 {
		println("polling disabled, will only execute once.")
		i.DoPoll()
		return
	}

	t := time.NewTicker(time.Duration(i.Interval) * time.Millisecond)

	for _ = range t.C {
		i.DoPoll()
	}
}

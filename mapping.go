package main

import (
	"encoding/json"
	"encoding/xml"
	"time"
)

//TODO: Fix json unmarshalling issues.  Current story only calls for XML.
type ApiSearchResult struct {
	XMLName      xml.Name   `xml:"response" json:"-"`
	Query        string     `xml:"query" json:"query"`
	Location     string     `xml:"location" json:"location"`
	DupeFilter   bool       `xml:"dupefilter" json:"dupefilter"`
	HighLight    bool       `xml:"highlight" json:"highlight"`
	TotalResults int        `xml:"totalresults" json:"totalresults"`
	Start        int        `xml:"start" json:"start"`
	End          int        `xml:"end" json:"end"`
	Radius       int        `xml:"radius" json:"radius"`
	PageNumber   int        `xml:"pageNumber" json:"pageNumber"`
	Results      JobResults `xml:"results" json:"results"`
}

type JobResults struct {
	XMLName       xml.Name    `xml:"results" json:"-"`
	JobResultList []JobResult `xml:"result"`
}

type JobResult struct {
	JobTitle              string     `xml:"jobtitle" json:"jobtitle"`
	Company               string     `xml:"company" json:"company"`
	City                  string     `xml:"city" json:"city"`
	State                 string     `xml:"state" json:"state"`
	Country               string     `xml:"country" json:"country"`
	FormattedLocation     string     `xml:"formattedLocation" json:"formattedLocation"`
	Source                string     `xml:"source" json:"source"`
	Date                  customTime `xml:"date" json:"date"`
	Snippet               string     `xml:"snippet" json:"snippet"`
	Url                   string     `xml:"url" json:"url"`
	OnMouseDown           string     `xml:"onmousedown" json:"onmousedown"`
	Latitude              float64    `xml:"latitude" json:"latitude"`
	Longitude             float64    `xml:"longitude" json:"longitude"`
	JobKey                string     `xml:"jobkey" json:"jobkey"`
	Sponsored             bool       `xml:"sponsored" json:"sponsored"`
	Expired               bool       `xml:"expired" json:"expired"`
	FormattedLocationFull string     `xml:"formattedLocationFull" json:"formattedLocationFull"`
	FormattedRelativeTime string     `xml:"formattedRelativeTime" json:"formattedRelativeTime"`
}

func (j *JobResult) GetDateString() string {
	return j.Date.Format(format)
}

//this anonymous struct is needed
//so that the date value can be properly
//parsed from the incoming xml.
type customTime struct {
	time.Time
}

//There are certain key words that the time package is looking for to parse
//a given date.  This looks like just a hard coded random date, but it's
//using the constants defined in the time package defined here:
// https://golang.org/src/time/format.go
const format = "Mon, 02 Jan 2006 15:04:05 MST"

func (c *customTime) UnmarshalXML(d *xml.Decoder, start xml.StartElement) error {
	text := ""
	d.DecodeElement(&text, &start)
	value, err := time.Parse(format, text)
	if err != nil {
		return err
	}
	*c = customTime{value}
	return nil
}

func (c *customTime) UnmarshalJSON(b []byte) error {
	text := ""
	err := json.Unmarshal(b, &text)
	if err != nil {
		return err
	}
	value, err := time.Parse(format, text)
	if err != nil {
		return err
	}
	*c = customTime{value}
	return nil
}

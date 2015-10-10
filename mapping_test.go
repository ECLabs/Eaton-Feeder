package main

import (
	"encoding/xml"
	"testing"
)

var (
	dummyResults = `
<?xml version="1.0" encoding="UTF-8" ?>
<response version="2">
    <query>java</query>
    <location>austin, tx</location>
    <dupefilter>true</dupefilter>
    <highlight>false</highlight>
    <totalresults>547</totalresults>
    <start>1</start>
    <end>10</end>
    <radius>25</radius>
    <pageNumber>0</pageNumber>
    <results>
        <result>
            <jobtitle>Java Developer</jobtitle>
            <company>XYZ Corp.</company>
            <city>Austin</city>
            <state>TX</state>
            <country>US</country>
            <formattedLocation>Austin, TX</formattedLocation>
            <source>Dice</source>
            <date>Tue, 03 Aug 2010 16:21:00 GMT</date>
            <snippet>looking for an object-oriented Java Developer... Java Servlets, HTML, JavaScript,
            AJAX, Struts, Struts2, JSF) desirable. Familiarity with Tomcat and the Java...</snippet>
            <url>http://www.indeed.com/viewjob?jk=12345&amp;indpubnum=8343699265155203</url>
            <onmousedown>indeed_clk(this,'0000');</onmousedown>
            <latitude>30.27127</latitude>
            <longitude>-97.74103</longitude>
            <jobkey>12345</jobkey>
            <sponsored>false</sponsored>
            <expired>false</expired>
            <formattedLocationFull>Austin, TX</formattedLocationFull>
            <formattedRelativeTime>11 hours ago</formattedRelativeTime>
        </result>
    </results>
</response>
`
)

func TestUnmarshalXML(t *testing.T) {
	result := new(ApiSearchResult)
	err := xml.Unmarshal([]byte(dummyResults), result)
	if err != nil {
		t.Fatal("failed to parse xml: ", err)
	}
	t.Log("Query: ", result.Query)
	if result.Query == "" {
		t.Fatal("no query set on response!", result.Query)
	}
	t.Log("Results: ", result)
	list := result.Results.JobResultList
	if list == nil || len(list) != 1 {
		t.Fatal("job result list is nil or empty!")
	}
}

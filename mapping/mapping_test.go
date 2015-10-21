package mapping

import (
	"encoding/xml"
	"testing"
	"time"
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
	dummyJobResult = `
<result><jobtitle>Patient Transporter</jobtitle><company>George Washington University hosp.</company><city>Washington</city><state>DC</state><country>US</country><formattedLocation>Washington, DC</formattedLocation><source>Indeed</source><date>Tue, 03 Aug 2010 16:21:00 GMT</date><snippet>Transporting patients to and from there rooms to doctors office and examines. Local candidates only:....</snippet><url>http://www.indeed.com/viewjob?jk=1cdf8d9d5da145f6&amp;qd=3shu1p8wbXm3aC55sNj4oTrLBxYcuXrnq_FHZnNIkdGdIOa58DTxWixnLmUlBYWlg7TZCxYhCgSQP1pdnoFFviS3QsJXL-tCKe4x9bjt7-Y&amp;indpubnum=5347954861571823&amp;atk=1a1gthcac5oskeok</url><onmousedown>indeed_clk(this, &#39;2484&#39;);</onmousedown><latitude>38.892857</latitude><longitude>-77.03297</longitude><jobkey>1cdf8d9d5da145f6</jobkey><sponsored>false</sponsored><expired>false</expired><formattedLocationFull>Washington, DC</formattedLocationFull><formattedRelativeTime>2 days ago</formattedRelativeTime></result>
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

func TestJobResultUnmarshalXML(t *testing.T) {
	j := new(JobResult)
	j.Date = customTime{time.Now()}
	data, err := xml.Marshal(j)
	if err != nil {
		t.Fatal("failed to marshal job result: ", err)
	}
	j = new(JobResult)
	err = xml.Unmarshal(data, j)

	if err != nil {
		t.Fatal("failed to unmarshal job result: ", err)
	}
	t.Log("Job Result Date: ", j.Date)
	err = xml.Unmarshal([]byte(dummyJobResult), j)

	if err != nil {
		t.Fatal("failed to unmarshal from dummy job result text: ", j)
	}
	t.Log("Job Result Date: ", j.Date)
}

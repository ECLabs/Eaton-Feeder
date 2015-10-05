package main

import (
	"net/url"
	"os"
	"testing"
)

func TestValidate(t *testing.T) {
	i := new(IndeedPoller)
	err := i.Validate()
	if err == nil {
		t.Fatal("publisher was not set, validate should fail!")
	}

	i.Publisher = "12345667"
	err = i.Validate()

	if err != nil {
		t.Fatal("validate should pass, publisher was set.")
	}
}

/*
More testing can be added for each value
set within the query string.  this seems
sufficient for now though.
*/
func TestGetUrl(t *testing.T) {
	i := new(IndeedPoller)
	i.Publisher = "1234567"
	i.BaseUrl = "http://api.indeed.com/api/?"
	i.SiteType = "jobsite"
	requestString := i.GetUrl()
	requestUrl, err := url.Parse(requestString)

	if err != nil {
		t.Fatal("failed to parse requestString", err)
	}

	values := requestUrl.Query()

	publishers := values["publisher"]

	if len(publishers) != 1 {
		t.Fatal("incorrect number of publishers were set!", publishers)
	}

	if publishers[0] != i.Publisher {
		t.Fatal("set publisher not existant in query string!", publishers[0], i.Publisher)
	}

	siteTypes := values["st"]

	if len(siteTypes) != 1 {
		t.Fatal("incorrect number of siteTypes set!", siteTypes)
	}

	if siteTypes[0] != i.SiteType {
		t.Fatal("set siteType does not match value in query string!", siteTypes[0], i.SiteType)
	}

	t.Log("Url: ", requestUrl)
}

func TestWriteFile(t *testing.T) {
	i := new(IndeedPoller)
	i.OutputFile = "output"
	i.Format = "xml"
	err := i.WriteResultsToFile([]byte("<results></results>\n"))
	if err != nil {
		t.Fatal("couldn't write to ", i.OutputFile, err)
	}
	os.Remove(i.GetFileName())
}

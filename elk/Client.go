package elk

import (
	. "github.com/KarmaPenny/golib/dynamics"

	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"time"
)

type Client struct {
	BaseUrl string
	HttpClient *http.Client
}

// Bulk executes a bulk operation
func (self *Client) Bulk(bulk_actions []interface{}) (*BulkResults, error) {
	results := BulkResults{}
	err := self.BulkRequest("POST", "/_bulk", bulk_actions, &results)
	return &results, err
}

func (self *Client) Push(updates BulkUpdate) (*BulkResults, error) {
	bulk_actions := Array{}
	for path := range updates {
		bulk_actions = append(bulk_actions, updates[path].Action())
		bulk_actions = append(bulk_actions, updates[path].Source())
	}
	results := BulkResults{}
	if len(bulk_actions) == 0 {
		return &results, nil
	}
	err := self.BulkRequest("POST", "/_bulk", bulk_actions, &results)
	return &results, err
}

// GetDocument returns the document with source fields identified by path
func (self *Client) GetDocument(path string) (*Document, error) {
	results := Document{}
	err := self.Request("GET", path, nil, &results)
	return &results, err
}

// GetRefreshInterval retrieves the refresh interval for an index
func (self *Client) GetRefreshInterval(index string) (time.Duration, error) {
	results := IndexSettingsResults{}
	url := fmt.Sprintf("/%s/_settings/index.refresh_interval", index)
	err := self.Request("GET", url, nil, &results)
	if err != nil {
		return time.Second, err
	}
	for index_name := range results {
		refresh_interval, err := time.ParseDuration(results[index_name].Settings.Index.RefreshInterval)
		if err != nil {
			return time.Second, err
		}
		return refresh_interval, nil
	}
	return time.Second, nil
}

// Index indexes a document
func (self *Client) Index(index string, id string, document interface{}) (*Document, error) {
	results := Document{}
	url := fmt.Sprintf("/%s/doc/%s", index, id)
	err := self.Request("PUT", url, document, &results)
	return &results, err
}

// MultiSearchTemplate executes a multisearch template request and returns the matching documents
func (self *Client) MultiSearchTemplate(index string, bulk_queries []interface{}) ([][]Document, error) {
	hits := [][]Document{}
	results := MultiSearchResults{}
	url := fmt.Sprintf("/%s/_msearch/template", index)
	err := self.BulkRequest("POST", url, bulk_queries, &results)
	if err != nil {
		return hits, err
	}
	for i := range results.Responses {
		hits = append(hits, results.Responses[i].Hits.Hits)
	}
	return hits, err
}

// BulkRequest sends a bulk request to the elasticsearch api
func (self *Client) BulkRequest(method string, url string, json_objects []interface{}, results interface{}) error {
	// create the request_body
	request_body := bytes.NewBuffer(make([]byte, 0))
	for i := range json_objects {
		data, err := json.Marshal(&json_objects[i])
		if err != nil {
			return err
		}
		request_body.Write(data)
		request_body.WriteRune('\n')
	}
	//fmt.Println(request_body.String())

	// create the request
	full_url := fmt.Sprintf("%s%s", self.BaseUrl, url)
	request, err := http.NewRequest(method, full_url, request_body)
	if err != nil {
		return err
	}

	// add content type header
	request.Header.Set("Content-Type", "application/x-ndjson")

	// do the request
	response, err := self.HttpClient.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()

	// read the response
	response_body, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return err
	}
	if response.StatusCode != 200  && response.StatusCode != 201 {
		return errors.New(fmt.Sprintf("StatusCode (%d): %s", response.StatusCode, response_body))
	}
	//fmt.Println(string(response_body))

	// decode response
	err = json.Unmarshal(response_body, results)
	if err != nil {
		return err
	}
	return nil
}

// Request sends a request to the elasticsearch api
func (self *Client) Request(method string, url string, json_object interface{}, results interface{}) error {
	// create the request_body
	var request_body *bytes.Buffer = bytes.NewBuffer(make([]byte, 0))
	if json_object != nil {
		data, err := json.Marshal(json_object)
		if err != nil {
			return err
		}
		request_body = bytes.NewBuffer(data)
	}

	// create the request
	full_url := fmt.Sprintf("%s%s", self.BaseUrl, url)
	request, err := http.NewRequest(method, full_url, request_body)
	if err != nil {
		return err
	}

	// add content-type header
	if json_object != nil {
		request.Header.Set("Content-Type", "application/json")
	}

	// do the request
	response, err := self.HttpClient.Do(request)
	if err != nil {
		return err
	}
	defer response.Body.Close()

	// read the response
	response_body, err := ioutil.ReadAll(response.Body)
	if err != nil {
		return err
	}
	if response.StatusCode != 200  && response.StatusCode != 201 {
		return errors.New(fmt.Sprintf("StatusCode (%d): %s", response.StatusCode, response_body))
	}

	// decode response
	err = json.Unmarshal(response_body, results)
	if err != nil {
		return err
	}
	return nil
}

// Search executes a query and returns the results
func (self *Client) Search(index string, query interface{}) ([]Document, error) {
	results := SearchResults{}
	url := fmt.Sprintf("/%s/_search", index)
	err := self.Request("POST", url, query, &results)
	if err != nil {
		return []Document{}, err
	}
	if results.TimedOut {
		took := time.Duration(results.Took) * time.Millisecond
		return []Document{}, errors.New(fmt.Sprintf("Search timed out after %s", took))
	}
	return results.Hits.Hits, nil
}

// Unlock releases the lock on the document identified by path
func (self *Client) Unlock(path string) error {
	update := Object{
		"doc": Object{
			"lock_until": "0",
			"lock_owner": "None",
		},
	}
	_, err := self.Update(path, &update)
	return err
}

// Update modifies a document
func (self *Client) Update(path string, update interface{}) (*Document, error) {
	results := Document{}
	url := fmt.Sprintf("%s/_update?retry_on_conflict=3", path)
	err := self.Request("POST", url, update, &results)
	return &results, err
}

// UpdateByQuery uses a query to update documents
func (self *Client) UpdateByQuery(index string, query interface{}) (*UpdateByQueryResults, error) {
	results := UpdateByQueryResults{}
	url := fmt.Sprintf("/%s/_update_by_query?conflicts=proceed", index)
	err := self.Request("POST", url, query, &results)
	return &results, err
}

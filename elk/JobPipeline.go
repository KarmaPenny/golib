package elk

import (
	. "github.com/KarmaPenny/golib/dynamics"

	"fmt"
	"log"
	"os"
	"time"
)

// JobPipeline type is used to concurrently process documents stored in elasticsearch
// Filter field determines what documents in the index to process
// Order field determines the order to process the documents
// NumWorkers field sets the number of threads to process documents with
// Task field is the function each document is passed to for processing
// TaskName field identifies the task for this pipeline inside elasticsearch
type JobPipeline struct {
	Client *Client
	Filter Object
	Index string
	NumWorkers int
	Order Array
	Task func(*Document)
	TaskName string

	host string
	id string
	last_update time.Time
	queue []*Document
	queue_index int
	refresh_interval time.Duration
	running_jobs map[string]bool
	slice_id int
	slice_max int
	workers []*Worker
}

// Start initializes the pipeline and fires up all the workers
func (self *JobPipeline) Start() {
	self.host, _ = os.Hostname()
	self.id = fmt.Sprintf("%s|%s", self.TaskName, self.host)
	self.refresh_interval = time.Second
	self.last_update = time.Now().Add(-1 * self.refresh_interval)
	self.queue = []*Document{}
	self.running_jobs = map[string]bool{}

	log.Printf("Starting %s", self.TaskName)
	self.workers = make([]*Worker, self.NumWorkers)
	for i := 0; i < self.NumWorkers; i++ {
		self.workers[i] = &Worker{Client: self.Client, Task: self.Task}
		self.workers[i].Start()
	}
}

// Stop tells all workers to stop and then waits for them to finish
func (self *JobPipeline) Stop() {
	log.Printf("Stopping %s", self.TaskName)
	for i := range(self.workers) {
		self.workers[i].Stop()
	}
	for i := range(self.workers) {
		self.workers[i].WaitForStop()
	}
	log.Printf("%s Stopped", self.TaskName)
}

// ProcessNext attempts to assign the next job on the queue to a worker
func (self *JobPipeline) Process() {
	// refresh the job queue every refresh interval
	if time.Since(self.last_update) > self.refresh_interval {
		self.refresh()
	}

	// if the queue is empty then wait for next refresh
	if self.queue_index >= len(self.queue) {
		time.Sleep(self.refresh_interval)
		return
	}

	// send the next job on the queue to the first available worker
	job := self.queue[self.queue_index]
	for i := 0; i < len(self.workers); i++ {
		if self.workers[i].SendJob(job) {
			self.running_jobs[job.Key()] = true
			self.queue_index++
			return
		}
	}

	// sleep for a short time if no workers were available
	time.Sleep(10 * time.Millisecond)
}

func (self *JobPipeline) refresh() {
	// get the refresh interval of the targeted index
	refresh_interval, err := self.Client.GetRefreshInterval(self.Index)
	if err != nil {
		log.Printf("[ERROR] unable to get %s refresh_interval: %s", self.Index, err)
		return
	}
	self.refresh_interval = refresh_interval

	// add or update host entry in list of workers
	expiration := Timestamp(time.Now().Add(10 * self.refresh_interval))
	worker := Object{
		"task": self.TaskName,
		"expires_at": expiration,
	}
	_, err = self.Client.Index("workers", self.id, &worker)
	if err != nil {
		log.Printf("[ERROR] unable to register worker: %s", err)
		return
	}

	// find all workers in cluster
	query := Object{
		"size": 10000,
		"_source": false,
		"sort": Array{
			Object{
				"_id": "asc",
			},
		},
		"query": Object{
			"bool": Object{
				"filter": Array{
					Object{
						"range": Object{
							"expires_at": Object{
								"gt": "now",
							},
						},
					},
					Object{
						"term": Object{
							"task": self.TaskName,
						},
					},
				},
			},
		},
	}
	results, err := self.Client.Search("workers", &query)
	if err != nil {
		log.Printf("[ERROR] failed to get list of workers in cluster: %s", err)
		return
	}

	// set slice max to number of workers in cluster
	self.slice_max = len(results)

	// set slice id to our position in the sorted list of workers
	self.slice_id = -1
	for i := 0; i < len(results); i++ {
		if results[i].Id == self.id {
			self.slice_id = i
			break
		}
	}

	// wait until we are in the list of workers
	if self.slice_id == -1 {
		return
	}

	// lock new jobs in our slice
	expiration = Timestamp(time.Now().Add(10 * self.refresh_interval))
	query = Object{
		"script": Object{
			"lang": "painless",
			"source": "ctx._source.lock_owner = params.lock_owner; ctx._source.lock_until = params.lock_until",
			"params": Object{
				"lock_owner": self.id,
				"lock_until": expiration,
			},
		},
		"query": Object{
			"bool": Object{
				"should": Array{
					Object{
						"range": Object{
							"lock_until": Object{
								"lt": "now",
							},
						},
					},
					Object{
						"bool": Object{
							"must_not": Object{
								"exists": Object{
									"field": "lock_until",
								},
							},
						},
					},
				},
				"filter": Array{
					self.Filter,
				},
				"minimum_should_match": 1,
			},
		},
	}
	if self.slice_max > 1 {
		query["slice"] = Object{
			"id": self.slice_id,
			"max": self.slice_max,
		}
	}
	_, err = self.Client.UpdateByQuery(self.Index, &query)
	if err != nil {
		log.Printf("[ERROR] unable to lock jobs: %s", err)
		return
	}

	// prevent locks we own from expiring
	expiration = Timestamp(time.Now().Add(10 * self.refresh_interval))
	query = Object{
		"script": Object{
			"lang": "painless",
			"source": "ctx._source.lock_owner = params.lock_owner; ctx._source.lock_until = params.lock_until",
			"params": Object{
				"lock_owner": self.id,
				"lock_until": expiration,
			},
		},
		"query": Object{
			"bool": Object{
				"filter": Array{
					Object{
						"term": Object{
							"lock_owner": self.id,
						},
					},
				},
			},
		},
	}
	_, err = self.Client.UpdateByQuery(self.Index, &query)
	if err != nil {
		log.Printf("[ERROR] unable to renew locks: %s", err)
		return
	}

	// find all jobs that are locked by us
	query = Object{
		"size": 10000,
		"_source": false,
		"sort": self.Order,
		"query": Object{
			"bool": Object{
				"filter": Array{
					Object{
						"term": Object{
							"lock_owner": self.id,
						},
					},
				},
			},
		},
	}
	results, err = self.Client.Search(self.Index, &query)
	if err != nil {
		log.Printf("[ERROR] failed to find locked jobs: %s", err)
		return
	}

	// rebuild queue from jobs that are not currently running
	new_running_jobs := map[string]bool{}
	self.queue = []*Document{}
	for i := range(results) {
		key := results[i].Key()
		if _, ok := self.running_jobs[key]; ok {
			new_running_jobs[key] = true
		} else {
			self.queue = append(self.queue, &results[i])
		}
	}
	self.running_jobs = new_running_jobs
	self.queue_index = 0

	// set lat update time
	self.last_update = time.Now()
}

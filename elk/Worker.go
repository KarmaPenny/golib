package elk

import (
	"log"
	"time"
)

type Worker struct {
	Client *Client
	Task func(*Document)

	stop chan bool
	stopped chan bool
	job chan *Document
}

// Start starts the worker
func (self *Worker) Start() {
	self.stop = make(chan bool, 1)
	self.stopped = make(chan bool, 1)
	self.job = make(chan *Document, 1)
	go self.run()
}

// run executes the task and then marks worker as stopped
func (self *Worker) run() {
	// report stopped when done
	defer self.finish()

	// keep chugging until told to stop
	for !self.stopping() {
		// attempt to get a job to process
		job := self.getJob()

		// there is no job to process so sleep a short while
		if job == nil {
			time.Sleep(10 * time.Millisecond)
			continue
		}

		// process the job
		self.execute_task(job)
	}
}

func (self *Worker) execute_task(job *Document) {
	// release lock on job when done
	defer self.Client.Unlock(job.Path())

	// load the source fields
	job, err := self.Client.GetDocument(job.Path())
	if err != nil {
		log.Printf("[ERROR] failed to load source fields of %s: %s", job.Path(), err)
		return
	}

	// execute task on job
	self.Task(job)
}

// finish marks the worker as stopped
func (self *Worker) finish() {
	self.stopped <- true
}

// Stopping returns true if the worker has been told to stop
func (self *Worker) stopping() bool {
	select {
		case <-self.stop:
			return true
		default:
			return false
	}
}

// Stop sends the stop signal to the worker
func (self *Worker) Stop() {
	self.stop <- true
}

// WaitForStop blocks until the worker is stopped
func (self *Worker) WaitForStop() {
	<-self.stopped
}

// SendJob attempts to send a job to the worker returns true if successful
func (self *Worker) SendJob(job *Document) bool {
	select {
		case self.job <- job:
			return true
		default:
			return false
	}
}

// GetJob attempts to receive a job, returns nil if no job found
func (self *Worker) getJob() *Document {
	select {
		case job := <-self.job:
			return job
		default:
			return nil
	}
}

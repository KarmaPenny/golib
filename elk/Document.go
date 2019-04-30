package elk

import (
	. "github.com/KarmaPenny/golib/dynamics"
	"fmt"
)

type Document struct {
	Shards Shards `json:"_shards"`
	Index string `json:"_index"`
	Type string `json:"_type"`
	Id string `json:"_id"`
	Version int `json:"_version"`
	SequenceNumber int `json:"_seq_no"`
	PrimaryTerm int `json:"_primary_term"`
	Result string `json:"result"`
	Found bool `json:"found"`
	Score float32 `json:"_score"`
	Source Object `json:"_source"`
	Status int `json:"status"`
}

// Key returns a uid for this document and version
func (self *Document) Key() string {
	return fmt.Sprintf("%s|%d", self.Id, self.Version)
}

// Path returns the path in elasticsearch to this document
func (self *Document) Path() string {
	return fmt.Sprintf("/%s/%s/%s", self.Index, self.Type, self.Id)
}

package redisearch

import (
	"sort"
)

// Document represents a single document to be indexed or returned from a query.
// Besides a score and id, the Properties are completely arbitrary
type Document struct {
	Id         string
	Score      float32
	Payload    []byte
	Properties map[string]interface{}
}

// NewDocument creates a document with the specific id and score
func NewDocument(id string, score float32) Document {
	return Document{
		Id:         id,
		Score:      score,
		Properties: make(map[string]interface{}),
	}
}

// SetPayload Sets the document payload
func (d *Document) SetPayload(payload []byte) {
	d.Payload = payload
}

// Set sets a property and its value in the document
func (d Document) Set(name string, value interface{}) Document {
	d.Properties[name] = value
	return d
}

// DocumentList is used to sort documents by descending score
type DocumentList []Document

func (l DocumentList) Len() int           { return len(l) }
func (l DocumentList) Swap(i, j int)      { l[i], l[j] = l[j], l[i] }
func (l DocumentList) Less(i, j int) bool { return l[i].Score > l[j].Score } //reverse sorting

// Sort the DocumentList
func (l DocumentList) Sort() {
	sort.Sort(l)
}

func (d *Document) EstimateSize() (sz int) {

	sz = len(d.Id)
	if d.Payload != nil {
		sz += len(d.Payload)
	}
	for k, v := range d.Properties {
		sz += len(k)
		switch s := v.(type) {
		case string:
			sz += len(s)
		case []byte:
			sz += len(s)
		case []rune:
			sz += len(s)
		}

	}
	return
}

package redisearch

import (
	"sort"
	"strings"
)

const (
	field_tokenization = ",.<>{}[]\"':;!@#$%^&*()-+=~"
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

// All punctuation marks and whitespaces (besides underscores) separate the document and queries into tokens.
// e.g. any character of `,.<>{}[]"':;!@#$%^&*()-+=~` will break the text into terms.
// So the text `foo-bar.baz...bag` will be tokenized into `[foo, bar, baz, bag]`
// Escaping separators in both queries and documents is done by prepending a backslash to any separator.
// e.g. the text `hello\-world hello-world` will be tokenized as `[hello-world, hello, world]`.
// **NOTE** that in most languages you will need an extra backslash when formatting the document or query,
// to signify an actual backslash, so the actual text in redis-cli for example, will be entered as `hello\\-world`.
// Underscores (`_`) are not used as separators in either document or query.
// So the text `hello_world` will remain as is after tokenization.
func EscapeTextFileString(value string) (string) {
	for _, char := range field_tokenization {
		value = strings.Replace(value, string(char), ("\\"+string(char)), -1 )
	}
	return value
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

package redisearch

import (
	"errors"
	"fmt"
	"strconv"

	"time"

	"log"

	"github.com/garyburd/redigo/redis"
)

// Options are flags passed to the the abstract Index call, which receives them as interface{}, allowing
// for implementation specific options
type Options struct {

	// If set, we will not save the documents contents, just index them, for fetching ids only
	NoSave bool

	NoFieldFlags bool

	NoFrequencies bool

	NoOffsetVectors bool

	Stopwords []string
}

// DefaultOptions represents the default options
var DefaultOptions = Options{
	NoSave:          false,
	NoFieldFlags:    false,
	NoFrequencies:   false,
	NoOffsetVectors: false,
	Stopwords:       nil,
}

// Cleint is an interface to redisearch's redis commands
type Client struct {
	pool *redis.Pool
	name string
}

var maxConns = 500

// NewClient creates a new client connecting to the redis host, and using the given name as key prefix
func NewClient(addr, name string) *Client {

	ret := &Client{
		pool: redis.NewPool(func() (redis.Conn, error) {
			// TODO: Add timeouts. and 2 separate pools for indexing and querying, with different timeouts
			return redis.Dial("tcp", addr)
		}, maxConns),
		name: name,
	}

	ret.pool.TestOnBorrow = func(c redis.Conn, t time.Time) (err error) {
		if time.Since(t) > time.Second {
			_, err = c.Do("PING")
		}
		return err
	}
	//ret.pool.MaxActive = ret.pool.MaxIdle

	return ret

}

// CreateIndex configues the index and creates it on redis
func (i *Client) CreateIndex(s *Schema) error {
	args := redis.Args{i.name}
	// Set flags based on options
	if s.Options.NoFieldFlags {
		args = append(args, "NOFIELDS")
	}
	if s.Options.NoFrequencies {
		args = append(args, "NOFREQS")
	}
	if s.Options.NoOffsetVectors {
		args = append(args, "NOOFFSETS")
	}
	if s.Options.Stopwords != nil {
		args = args.Add("STOPWORDS", len(s.Options.Stopwords))
		if len(s.Options.Stopwords) > 0 {
			args = args.AddFlat(s.Options.Stopwords)
		}
	}

	args = append(args, "SCHEMA")
	for _, f := range s.Fields {

		switch f.Type {
		case TextField:

			args = append(args, f.Name, "TEXT")
			if f.Options != nil {
				opts, ok := f.Options.(TextFieldOptions)
				if !ok {
					return errors.New("Invalid text field options type")
				}

				args = append(args, "WEIGHT", opts.Weight)

				if opts.Sortable {
					args = append(args, "SORTABLE")
				}

			}

		case NumericField:
			args = append(args, f.Name, "NUMERIC")
			if f.Options != nil {
				opts, ok := f.Options.(NumericFieldOptions)
				if !ok {
					return errors.New("Invalid numeric field options type")
				}

				if opts.Sortable {
					args = append(args, "SORTABLE")
				}

			}
		case NoIndexField:
			continue

		default:
			return fmt.Errorf("Unsupported field type %v", f.Type)
		}

	}

	conn := i.pool.Get()
	defer conn.Close()
	_, err := conn.Do("FT.CREATE", args...)
	return err
}

// IndexingOptions represent the options for indexing a single document
type IndexingOptions struct {
	Language string
	NoSave   bool
	Replace  bool
}

// DefaultIndexingOptions are the default options for document indexing
var DefaultIndexingOptions = IndexingOptions{
	Language: "",
	NoSave:   false,
	Replace:  false,
}

// Index indexes multiple documents on the index, with optional Options passed to options
func (i *Client) IndexOptions(opts IndexingOptions, docs ...Document) (errors map[int]error) {

	conn := i.pool.Get()
	defer conn.Close()

	n := 0
	errors = make(map[int]error)

	for ii, doc := range docs {
		args := make(redis.Args, 0, 6+len(doc.Properties))
		args = append(args, i.name, doc.Id, doc.Score)
		// apply options
		if opts.NoSave {
			args = append(args, "NOSAVE")
		}
		if opts.Language != "" {
			args = append(args, "LANGUAGE", opts.Language)
		}
		if opts.Replace {
			args = append(args, "REPLACE")
		}

		if doc.Payload != nil {
			args = args.Add("PAYLOAD", doc.Payload)
		}

		args = append(args, "FIELDS")

		for k, f := range doc.Properties {
			args = append(args, k, f)
		}

		if err := conn.Send("FT.ADD", args...); err != nil {
			errors[ii] = err
			return
		}
		n++
	}

	if err := conn.Flush(); err != nil {
		errors[-1] = err
		return
	}

	for n > 0 {
		if _, err := conn.Receive(); err != nil {
			errors[n-1] = err
		}
		n--
	}

	if len(errors) == 0 {
		return nil
	}

	return
}

// convert the result from a redis query to a proper Document object
func loadDocument(arr []interface{}, idIdx, scoreIdx, payloadIdx, fieldsIdx int) (Document, error) {

	var score float64 = 1
	var err error
	if scoreIdx > 0 {
		if score, err = strconv.ParseFloat(string(arr[idIdx+scoreIdx].([]byte)), 64); err != nil {
			return Document{}, fmt.Errorf("Could not parse score: %s", err)
		}
	}

	doc := NewDocument(string(arr[idIdx].([]byte)), float32(score))

	if payloadIdx > 0 {
		doc.Payload, _ = arr[idIdx+payloadIdx].([]byte)
	}

	if fieldsIdx > 0 {
		lst := arr[idIdx+fieldsIdx].([]interface{})
		for i := 0; i < len(lst); i += 2 {
			prop := string(lst[i].([]byte))
			var val interface{}
			switch v := lst[i+1].(type) {
			case []byte:
				val = string(v)
			default:
				val = v

			}
			doc = doc.Set(prop, val)
		}
	}

	return doc, nil
}

func (i *Client) Index(docs ...Document) map[int]error {
	return i.IndexOptions(DefaultIndexingOptions, docs...)
}

// Search searches the index for the given query, and returns documents,
// the total number of results, or an error if something went wrong
func (i *Client) Search(q *Query) (docs []Document, total int, err error) {
	conn := i.pool.Get()
	defer conn.Close()

	args := redis.Args{i.name}
	args = append(args, q.serialize()...)

	res, err := redis.Values(conn.Do("FT.SEARCH", args...))
	if err != nil {
		return
	}

	if total, err = redis.Int(res[0], nil); err != nil {
		return
	}

	docs = make([]Document, 0, len(res)-1)

	skip := 1
	scoreIdx := -1
	fieldsIdx := -1
	payloadIdx := -1
	if q.Flags&QueryWithScores != 0 {
		scoreIdx = 1
		skip++
	}
	if q.Flags&QueryWithPayloads != 0 {
		payloadIdx = skip
		skip++
	}

	if q.Flags&QueryNoContent == 0 {
		fieldsIdx = skip
		skip++
	}

	if len(res) > skip {
		for i := 1; i < len(res); i += skip {

			if d, e := loadDocument(res, i, scoreIdx, payloadIdx, fieldsIdx); e == nil {
				docs = append(docs, d)
			} else {
				log.Print("Error parsing doc: ", e)
			}
		}
	}
	return
}

// Drop the  Currentl just flushes the DB - note that this will delete EVERYTHING on the redis instance
func (i *Client) Drop() error {
	conn := i.pool.Get()
	defer conn.Close()

	_, err := conn.Do("FT.DROP", i.name)
	return err

}

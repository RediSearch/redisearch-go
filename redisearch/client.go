package redisearch

import (
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"

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

// Client is an interface to redisearch's redis commands
type Client struct {
	pool ConnPool
	name string
}

var maxConns = 500

// NewClient creates a new client connecting to the redis host, and using the given name as key prefix.
// Addr can be a single host:port pair, or a comma separated list of host:port,host:port...
// In the case of multiple hosts we create a multi-pool and select connections at random
func NewClient(addr, name string) *Client {

	addrs := strings.Split(addr, ",")
	var pool ConnPool
	if len(addrs) == 1 {
		pool = NewSingleHostPool(addrs[0])
	} else {
		pool = NewMultiHostPool(addrs)
	}
	ret := &Client{
		pool: pool,
		name: name,
	}

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

				if opts.Weight != 0 && opts.Weight != 1 {
					args = append(args, "WEIGHT", opts.Weight)
				}
				if opts.NoStem {
					args = append(args, "NOSTEM")
				}

				if opts.Sortable {
					args = append(args, "SORTABLE")
				}

				if opts.NoIndex {
					args = append(args, "NOINDEX")
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
				if opts.NoIndex {
					args = append(args, "NOINDEX")
				}
			}
		case TagField:
			args = append(args, f.Name, "TAG")
			if f.Options != nil {
				opts, ok := f.Options.(TagFieldOptions)
				if !ok {
					return errors.New("Invalid tag field options type")
				}
				if opts.Separator != 0 {
					args = append(args, "SEPARATOR", fmt.Sprintf("%c", opts.Separator))

				}
				if opts.Sortable {
					args = append(args, "SORTABLE")
				}
				if opts.NoIndex {
					args = append(args, "NOINDEX")
				}
			}
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
	Partial  bool
}

// DefaultIndexingOptions are the default options for document indexing
var DefaultIndexingOptions = IndexingOptions{
	Language: "",
	NoSave:   false,
	Replace:  false,
	Partial:  false,
}

// IndexOptions indexes multiple documents on the index, with optional Options passed to options
func (i *Client) IndexOptions(opts IndexingOptions, docs ...Document) error {

	conn := i.pool.Get()
	defer conn.Close()

	n := 0
	var merr MultiError

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

		if opts.Partial {
			opts.Replace = true
		}

		if opts.Replace {
			args = append(args, "REPLACE")
			if opts.Partial {
				args = append(args, "PARTIAL")
			}
		}

		if doc.Payload != nil {
			args = args.Add("PAYLOAD", doc.Payload)
		}

		args = append(args, "FIELDS")

		for k, f := range doc.Properties {
			args = append(args, k, f)
		}

		if err := conn.Send("FT.ADD", args...); err != nil {
			if merr == nil {
				merr = NewMultiError(len(docs))
			}
			merr[ii] = err

			return merr
		}
		n++
	}

	if err := conn.Flush(); err != nil {
		return err
	}

	for n > 0 {
		if _, err := conn.Receive(); err != nil {
			if merr == nil {
				merr = NewMultiError(len(docs))
			}
			merr[n-1] = err
		}
		n--
	}

	if merr == nil {
		return nil
	}

	return merr
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

// Index indexes a list of documents with the default options
func (i *Client) Index(docs ...Document) error {
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

// Explain Return a textual string explaining the query
func (i *Client) Explain(q *Query) (string, error) {
	conn := i.pool.Get()
	defer conn.Close()

	args := redis.Args{i.name}
	args = append(args, q.serialize()...)

	return redis.String(conn.Do("FT.EXPLAIN", args...))
}

// Drop the  Currentl just flushes the DB - note that this will delete EVERYTHING on the redis instance
func (i *Client) Drop() error {
	conn := i.pool.Get()
	defer conn.Close()

	_, err := conn.Do("FT.DROP", i.name)
	return err

}

// IndexInfo - Structure showing information about an existing index
type IndexInfo struct {
	Schema               Schema
	Name                 string  `redis:"index_name"`
	DocCount             uint64  `redis:"num_docs"`
	RecordCount          uint64  `redis:"num_records"`
	TermCount            uint64  `redis:"num_terms"`
	MaxDocID             uint64  `redis:"max_doc_id"`
	InvertedIndexSizeMB  float64 `redis:"inverted_sz_mb"`
	OffsetVectorSizeMB   float64 `redis:"offset_vector_sz_mb"`
	DocTableSizeMB       float64 `redis:"doc_table_size_mb"`
	KeyTableSizeMB       float64 `redis:"key_table_size_mb"`
	RecordsPerDocAvg     float64 `redis:"records_per_doc_avg"`
	BytesPerRecordAvg    float64 `redis:"bytes_per_record_avg"`
	OffsetsPerTermAvg    float64 `redis:"offsets_per_term_avg"`
	OffsetBitsPerTermAvg float64 `redis:"offset_bits_per_record_avg"`
}

func (info *IndexInfo) setTarget(key string, value interface{}) error {
	v := reflect.ValueOf(info).Elem()
	for i := 0; i < v.NumField(); i++ {
		tag := v.Type().Field(i).Tag.Get("redis")
		if tag == key {
			targetInfo := v.Field(i)
			switch targetInfo.Kind() {
			case reflect.String:
				s, _ := redis.String(value, nil)
				targetInfo.SetString(s)
			case reflect.Uint64:
				u, _ := redis.Uint64(value, nil)
				targetInfo.SetUint(u)
			case reflect.Float64:
				f, _ := redis.Float64(value, nil)
				targetInfo.SetFloat(f)
			default:
				panic("Tag set without handler")
			}
			return nil
		}
	}
	return errors.New("setTarget: No handler defined for :" + key)
}

func sliceIndex(haystack []string, needle string) int {
	for pos, elem := range haystack {
		if elem == needle {
			return pos
		}
	}
	return -1
}

func (info *IndexInfo) loadSchema(values []interface{}, options []string) {
	// Values are a list of fields
	scOptions := Options{}
	for _, opt := range options {
		switch strings.ToUpper(opt) {
		case "NOFIELDS":
			scOptions.NoFieldFlags = true
		case "NOFREQS":
			scOptions.NoFrequencies = true
		case "NOOFFSETS":
			scOptions.NoOffsetVectors = true
		}
	}
	sc := NewSchema(scOptions)
	for _, specTmp := range values {
		// spec, isArr := specTmp.([]string)
		// if !isArr {
		// 	panic("Value is not an array of strings!")
		// }
		rawSpec, err := redis.Values(specTmp, nil)
		if err != nil {
			log.Printf("Warning: Couldn't read schema. %s\n", err.Error())
			continue
		}
		spec := make([]string, 0)

		// Convert all to string, if not already string
		for _, elem := range rawSpec {
			s, isString := elem.(string)
			if !isString {
				s, err = redis.String(elem, err)
				if err != nil {
					log.Printf("Warning: Couldn't read schema. %s\n", err.Error())
					continue
				}
			}
			spec = append(spec, s)
		}
		// Name, Type,
		if len(spec) < 3 {
			log.Printf("Invalid spec")
			continue
		}

		var options []string
		if len(spec) > 3 {
			options = spec[3:]
		} else {
			options = []string{}
		}

		f := Field{Name: spec[0]}
		switch strings.ToUpper(spec[2]) {
		case "NUMERIC":
			f.Type = NumericField
			nfOptions := NumericFieldOptions{}
			f.Options = nfOptions
			if sliceIndex(options, "SORTABLE") != -1 {
				nfOptions.Sortable = true
			}
		case "TEXT":
			f.Type = TextField
			tfOptions := TextFieldOptions{}
			f.Options = tfOptions
			if sliceIndex(options, "SORTABLE") != -1 {
				tfOptions.Sortable = true
			}
			if wIdx := sliceIndex(options, "WEIGHT"); wIdx != -1 && wIdx+1 != len(spec) {
				weightString := options[wIdx+1]
				weight64, _ := strconv.ParseFloat(weightString, 32)
				tfOptions.Weight = float32(weight64)
			}
		}
		sc = sc.AddField(f)
	}
	info.Schema = *sc
}

// Info - Get information about the index. This can also be used to check if the
// index exists
func (i *Client) Info() (*IndexInfo, error) {
	conn := i.pool.Get()
	defer conn.Close()

	res, err := redis.Values(conn.Do("FT.INFO", i.name))
	if err != nil {
		return nil, err
	}

	ret := IndexInfo{}
	var schemaFields []interface{}
	var indexOptions []string

	// Iterate over the values
	for ii := 0; ii < len(res); ii += 2 {
		key, _ := redis.String(res[ii], nil)
		if err := ret.setTarget(key, res[ii+1]); err == nil {
			continue
		}

		switch key {
		case "index_options":
			indexOptions, _ = redis.Strings(res[ii+1], nil)
		case "fields":
			schemaFields, _ = redis.Values(res[ii+1], nil)
		}
	}

	if schemaFields != nil {
		ret.loadSchema(schemaFields, indexOptions)
	}

	return &ret, nil
}

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
func (i *Client) CreateIndex(s *Schema) (err error) {
	args := redis.Args{i.name}
	// Set flags based on options
	args, err = SerializeSchema(s, args)
	if err != nil {
		return
	}

	conn := i.pool.Get()
	defer conn.Close()
	_, err = conn.Do("FT.CREATE", args...)
	return err
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
			var prop string
			switch lst[i].(type) {
			case []byte:
				prop = string(lst[i].([]byte))
			default:
				prop = lst[i].(string)
			}

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
	args = append(args, q.Serialize()...)

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

// SpellCheck performs spelling correction on a query, returning suggestions for misspelled terms,
// the total number of results, or an error if something went wrong
func (i *Client) SpellCheck(q *Query, s *SpellCheckOptions) (suggs []MisspelledTerm, total int, err error) {
	conn := i.pool.Get()
	defer conn.Close()

	args := redis.Args{i.name}
	args = append(args, q.Serialize()...)
	args = append(args, s.serialize()...)

	res, err := redis.Values(conn.Do("FT.SPELLCHECK", args...))
	if err != nil {
		return
	}
	total = 0
	suggs = make([]MisspelledTerm, 0)

	// Each misspelled term, in turn, is a 3-element array consisting of
	// - the constant string "TERM" ( 3-element position 0 -- we dont use it )
	// - the term itself ( 3-element position 1 )
	// - an array of suggestions for spelling corrections ( 3-element position 2 )
	termIdx := 1
	suggIdx := 2
	for i := 0; i < len(res); i++ {
		var termArray []interface{} = nil
		termArray, err = redis.Values(res[i], nil)
		if err != nil {
			return
		}

		if d, e := loadMisspelledTerm(termArray, termIdx, suggIdx); e == nil {
			suggs = append(suggs, d)
			if d.Len() > 0 {
				total++
			}
		} else {
			log.Print("Error parsing misspelled suggestion: ", e)
		}
	}

	return
}

// Aggregate
func (i *Client) Aggregate(q *AggregateQuery) (aggregateReply [][]string, total int, err error) {
	conn := i.pool.Get()
	defer conn.Close()
	hasCursor := q.WithCursor
	validCursor := q.CursorHasResults()
	var res []interface{} = nil
	if ! validCursor {
		args := redis.Args{i.name}
		args = append(args, q.Serialize()...)
		res, err = redis.Values(conn.Do("FT.AGGREGATE", args...))
	} else {
		args := redis.Args{"READ", i.name, q.Cursor.Id}
		res, err = redis.Values(conn.Do("FT.CURSOR", args...))
	}
	if err != nil {
		return
	}
	// has no cursor
	if ! hasCursor {
		total = len(res) - 1
		if total > 1 {
			aggregateReply = ProcessAggResponse(res[1:])
		}
		// has cursor
	} else {
		var partialResults, err = redis.Values(res[0], nil)
		if err != nil {
			return aggregateReply, total, err
		}
		q.Cursor.Id, err = redis.Int(res[1], nil)
		if err != nil {
			return aggregateReply, total, err
		}
		total = len(partialResults) - 1
		if total > 1 {
			aggregateReply = ProcessAggResponse(partialResults[1:])
		}
	}

	return
}

// Explain Return a textual string explaining the query
func (i *Client) Explain(q *Query) (string, error) {
	conn := i.pool.Get()
	defer conn.Close()

	args := redis.Args{i.name}
	args = append(args, q.Serialize()...)

	return redis.String(conn.Do("FT.EXPLAIN", args...))
}

// Drop the  Currentl just flushes the DB - note that this will delete EVERYTHING on the redis instance
func (i *Client) Drop() error {
	conn := i.pool.Get()
	defer conn.Close()

	_, err := conn.Do("FT.DROP", i.name)
	return err

}

// Delete the document from the index, optionally delete the actual document
func (i *Client) Delete(docId string, deleteDocument bool) (err error) {
	conn := i.pool.Get()
	defer conn.Close()

	if deleteDocument {
		_, err = conn.Do("FT.DEL", i.name, docId)
	} else {
		_, err = conn.Do("FT.DEL", i.name, docId, "DD")
	}

	return
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

// Delete deletes the Autocompleter key for this AC
func (i *Client) AutocompleterDelete(a *Autocompleter) error {
	conn := i.pool.Get()
	defer conn.Close()

	_, err := conn.Do("DEL", a.name)
	return err
}

// AddTerms pushes new term suggestions to the index
func (c *Client) AutocompleterAddTerms(a *Autocompleter, terms ...Suggestion) error {
	conn := c.pool.Get()
	defer conn.Close()

	i := 0
	for _, term := range terms {

		args := redis.Args{a.name, term.Term, term.Score}
		if term.Payload != "" {
			args = append(args, "PAYLOAD", term.Payload)
		}

		if err := conn.Send("FT.SUGADD", args...); err != nil {
			return err
		}
		i++
	}
	if err := conn.Flush(); err != nil {
		return err
	}
	for i > 0 {
		if _, err := conn.Receive(); err != nil {
			return err
		}
		i--
	}
	return nil
}

// AddTerms pushes new term suggestions to the index
func (c *Client) AutocompleterDeleteTerms(a *Autocompleter, terms ...Suggestion) error {
	conn := c.pool.Get()
	defer conn.Close()

	i := 0
	for _, term := range terms {

		args := redis.Args{a.name, term.Term}
		if err := conn.Send("FT.SUGDEL", args...); err != nil {
			return err
		}
		i++
	}
	if err := conn.Flush(); err != nil {
		return err
	}
	for i > 0 {
		if _, err := conn.Receive(); err != nil {
			return err
		}
		i--
	}
	return nil
}

// AddTerms pushes new term suggestions to the index
func (c *Client) AutocompleterLength(a *Autocompleter) (len int64, err error) {
	conn := c.pool.Get()
	defer conn.Close()
	len, err = redis.Int64(conn.Do("FT.SUGLEN", a.name))
	return
}

// Suggest gets completion suggestions from the Autocompleter dictionary to the given prefix.
// If fuzzy is set, we also complete for prefixes that are in 1 Levenshten distance from the
// given prefix
//
// Deprecated: Please use SuggestOpts() instead
func (c *Client) AutocompleterSuggest(a *Autocompleter, prefix string, num int, fuzzy bool) (ret []Suggestion, err error) {
	conn := c.pool.Get()
	defer conn.Close()

	seropts := DefaultSuggestOptions
	seropts.Num = num
	seropts.Fuzzy = fuzzy
	args, inc := a.Serialize(prefix, seropts)

	vals, err := redis.Strings(conn.Do("FT.SUGGET", args...))
	if err != nil {
		return nil, err
	}

	ret = ProcessSugGetVals(vals, inc, true, false)

	return
}

// SuggestOpts gets completion suggestions from the Autocompleter dictionary to the given prefix.
// SuggestOptions are passed allowing you specify if the returned values contain a payload, and scores.
// If SuggestOptions.Fuzzy is set, we also complete for prefixes that are in 1 Levenshten distance from the
// given prefix
func (c *Client) AutocompleterSuggestOpts(a *Autocompleter, prefix string, opts SuggestOptions) (ret []Suggestion, err error) {
	conn := c.pool.Get()
	defer conn.Close()

	args, inc := a.Serialize(prefix, opts)
	vals, err := redis.Strings(conn.Do("FT.SUGGET", args...))
	if err != nil {
		return nil, err
	}

	ret = ProcessSugGetVals(vals, inc, opts.WithScores, opts.WithPayloads)

	return
}

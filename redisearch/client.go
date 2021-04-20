package redisearch

import (
	"errors"
	"log"
	"reflect"
	"strconv"
	"strings"

	"github.com/gomodule/redigo/redis"
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

// NewClientFromPool creates a new Client with the given pool and index name
func NewClientFromPool(pool *redis.Pool, name string) *Client {
	ret := &Client{
		pool: pool,
		name: name,
	}
	return ret
}

// CreateIndex configures the index and creates it on redis
func (i *Client) CreateIndex(schema *Schema) (err error) {
	return i.indexWithDefinition(i.name, schema, nil)
}

// CreateIndexWithIndexDefinition configures the index and creates it on redis
// IndexDefinition is used to define a index definition for automatic indexing on Hash update
func (i *Client) CreateIndexWithIndexDefinition(schema *Schema, definition *IndexDefinition) (err error) {
	return i.indexWithDefinition(i.name, schema, definition)
}

// internal method
func (i *Client) indexWithDefinition(indexName string, schema *Schema, definition *IndexDefinition) (err error) {
	args := redis.Args{indexName}
	if definition != nil {
		args = definition.Serialize(args)
	}
	// Set flags based on options
	args, err = SerializeSchema(schema, args)
	if err != nil {
		return
	}
	conn := i.pool.Get()
	defer conn.Close()
	_, err = conn.Do("FT.CREATE", args...)
	return
}

// AddField Adds a new field to the index.
func (i *Client) AddField(f Field) error {
	args := redis.Args{i.name}
	args = append(args, "SCHEMA", "ADD")
	args, err := serializeField(f, args)
	if err != nil {
		return err
	}
	conn := i.pool.Get()
	defer conn.Close()
	_, err = conn.Do("FT.ALTER", args...)
	return err
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

// Adds an alias to an index.
func (i *Client) AliasAdd(name string) (err error) {
	conn := i.pool.Get()
	defer conn.Close()
	args := redis.Args{name}.Add(i.name)
	_, err = redis.String(conn.Do("FT.ALIASADD", args...))
	return
}

// Deletes an alias to an index.
func (i *Client) AliasDel(name string) (err error) {
	conn := i.pool.Get()
	defer conn.Close()
	args := redis.Args{name}
	_, err = redis.String(conn.Do("FT.ALIASDEL", args...))
	return
}

// Deletes an alias to an index.
func (i *Client) AliasUpdate(name string) (err error) {
	conn := i.pool.Get()
	defer conn.Close()
	args := redis.Args{name}.Add(i.name)
	_, err = redis.String(conn.Do("FT.ALIASUPDATE", args...))
	return
}

// Adds terms to a dictionary.
func (i *Client) DictAdd(dictionaryName string, terms []string) (newTerms int, err error) {
	conn := i.pool.Get()
	defer conn.Close()
	newTerms = 0
	args := redis.Args{dictionaryName}.AddFlat(terms)
	newTerms, err = redis.Int(conn.Do("FT.DICTADD", args...))
	return
}

// Deletes terms from a dictionary
func (i *Client) DictDel(dictionaryName string, terms []string) (deletedTerms int, err error) {
	conn := i.pool.Get()
	defer conn.Close()
	deletedTerms = 0
	args := redis.Args{dictionaryName}.AddFlat(terms)
	deletedTerms, err = redis.Int(conn.Do("FT.DICTDEL", args...))
	return
}

// Dumps all terms in the given dictionary.
func (i *Client) DictDump(dictionaryName string) (terms []string, err error) {
	conn := i.pool.Get()
	defer conn.Close()
	args := redis.Args{dictionaryName}
	terms, err = redis.Strings(conn.Do("FT.DICTDUMP", args...))
	return
}

// SpellCheck performs spelling correction on a query, returning suggestions for misspelled terms,
// the total number of results, or an error if something went wrong
func (i *Client) SpellCheck(q *Query, s *SpellCheckOptions) (suggs []MisspelledTerm, total int, err error) {
	conn := i.pool.Get()
	defer conn.Close()

	args := redis.Args{i.name}
	args = append(args, q.serialize()...)
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
	if !validCursor {
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
	if !hasCursor {
		total, aggregateReply, err = processAggReply(res)
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
		total, aggregateReply, err = processAggReply(partialResults)
	}

	return
}

// Get - Returns the full contents of a document
func (i *Client) Get(docId string) (doc *Document, err error) {
	doc = nil
	conn := i.pool.Get()
	defer conn.Close()
	var reply interface{}
	args := redis.Args{i.name, docId}
	reply, err = conn.Do("FT.GET", args...)
	if reply != nil {
		var array_reply []interface{}
		array_reply, err = redis.Values(reply, err)
		if err != nil {
			return
		}
		if len(array_reply) > 0 {
			document := NewDocument(docId, 1)
			document.loadFields(array_reply)
			doc = &document
		}
	}
	return
}

// MultiGet - Returns the full contents of multiple documents.
// Returns an array with exactly the same number of elements as the number of keys sent to the command.
// Each element in it is either an Document or nil if it was not found.
func (i *Client) MultiGet(documentIds []string) (docs []*Document, err error) {
	docs = make([]*Document, len(documentIds))
	conn := i.pool.Get()
	defer conn.Close()
	var reply interface{}
	args := redis.Args{i.name}.AddFlat(documentIds)
	reply, err = conn.Do("FT.MGET", args...)
	if reply != nil {
		var array_reply []interface{}
		array_reply, err = redis.Values(reply, err)
		if err != nil {
			return
		}
		for i := 0; i < len(array_reply); i++ {

			if array_reply[i] != nil {
				var innerArray []interface{}
				innerArray, err = redis.Values(array_reply[i], nil)
				if err != nil {
					return
				}
				if len(array_reply) > 0 {
					document := NewDocument(documentIds[i], 1)
					document.loadFields(innerArray)
					docs[i] = &document
				}
			} else {
				docs[i] = nil
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

//  Deletes the index and all the keys associated with it.
func (i *Client) Drop() error {
	conn := i.pool.Get()
	defer conn.Close()

	_, err := conn.Do("FT.DROP", i.name)
	return err

}

// Deletes the secondary index and optionally the associated hashes
//
// Available since RediSearch 2.0.
//
// By default, DropIndex() which is a wrapper for RediSearch FT.DROPINDEX does not delete the document hashes associated with the index.
// Setting the argument deleteDocuments to true deletes the hashes as well.
func (i *Client) DropIndex(deleteDocuments bool) error {
	conn := i.pool.Get()
	defer conn.Close()
	var err error = nil
	if deleteDocuments {
		_, err = conn.Do("FT.DROPINDEX", i.name, "DD")
	} else {
		_, err = conn.Do("FT.DROPINDEX", i.name)
	}
	return err
}

// Delete the document from the index, optionally delete the actual document
// WARNING: As of RediSearch 2.0 and above, FT.DEL always deletes the underlying document.
// Deprecated: This function  is deprecated on RediSearch 2.0 and above, use DeleteDocument() instead
func (i *Client) Delete(docId string, deleteDocument bool) (err error) {
	return i.delDoc(docId, deleteDocument)
}

// Delete the document from the index and also delete the HASH key in which the document is stored
func (i *Client) DeleteDocument(docId string) (err error) {
	return i.delDoc(docId, true)
}

// Internal method to be used by Delete() and DeleteDocument()
func (i *Client) delDoc(docId string, deleteDocument bool) (err error) {
	conn := i.pool.Get()
	defer conn.Close()
	if deleteDocument {
		_, err = conn.Do("FT.DEL", i.name, docId, "DD")
	} else {
		_, err = conn.Do("FT.DEL", i.name, docId)
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
			case reflect.Bool:
				f, _ := redis.Uint64(value, nil)
				if f == 0 {
					targetInfo.SetBool(false)
				} else {
					targetInfo.SetBool(true)
				}
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

// Set runtime configuration option
func (i *Client) SetConfig(option string, value string) (string, error) {
	conn := i.pool.Get()
	defer conn.Close()

	args := redis.Args{"SET", option, value}
	return redis.String(conn.Do("FT.CONFIG", args...))
}

// Get runtime configuration option value
func (i *Client) GetConfig(option string) (map[string]string, error) {
	conn := i.pool.Get()
	defer conn.Close()

	args := redis.Args{"GET", option}
	values, err := redis.Values(conn.Do("FT.CONFIG", args...))
	if err != nil {
		return nil, err
	}

	m := make(map[string]string)
	valLen := len(values)
	for i := 0; i < valLen; i++ {
		kvs, _ := redis.Strings(values[i], nil)
		if kvs != nil && len(kvs) == 2 {
			m[kvs[0]] = kvs[1]
		}
	}
	return m, nil
}

// Get the distinct tags indexed in a Tag field
func (i *Client) GetTagVals(index string, filedName string) ([]string, error) {
	conn := i.pool.Get()
	defer conn.Close()

	args := redis.Args{index, filedName}
	return redis.Strings(conn.Do("FT.TAGVALS", args...))
}

// Adds a synonym group.
// Deprecated: This function  is not longer supported on RediSearch 2.0 and above, use SynUpdate instead
func (i *Client) SynAdd(indexName string, terms []string) (int64, error) {
	conn := i.pool.Get()
	defer conn.Close()

	args := redis.Args{indexName}.AddFlat(terms)
	return redis.Int64(conn.Do("FT.SYNADD", args...))
}

// Updates a synonym group, with additional terms.
func (i *Client) SynUpdate(indexName string, synonymGroupId int64, terms []string) (string, error) {
	conn := i.pool.Get()
	defer conn.Close()

	args := redis.Args{indexName, synonymGroupId}.AddFlat(terms)
	return redis.String(conn.Do("FT.SYNUPDATE", args...))
}

// Dumps the contents of a synonym group.
func (i *Client) SynDump(indexName string) (map[string][]int64, error) {
	conn := i.pool.Get()
	defer conn.Close()

	args := redis.Args{indexName}
	values, err := redis.Values(conn.Do("FT.SYNDUMP", args...))
	if err != nil {
		return nil, err
	}

	valLen := len(values)
	if valLen%2 != 0 {
		return nil, errors.New("SynDump: expects even number of values result")
	}

	m := make(map[string][]int64, valLen/2)
	for i := 0; i < valLen; i += 2 {
		key := values[i].([]byte)
		gids, err := redis.Int64s(values[i+1], nil)
		if err != nil {
			return nil, err
		}
		m[string(key)] = gids
	}
	return m, nil
}

// Adds a document to the index from an existing HASH key in Redis.
// Deprecated: This function  is not longer supported on RediSearch 2.0 and above, use HSET instead
// See the example ExampleClient_CreateIndexWithIndexDefinition for a deeper understanding on how to move towards using hashes on your application
func (i *Client) AddHash(docId string, score float32, language string, replace bool) (string, error) {
	conn := i.pool.Get()
	defer conn.Close()

	args := redis.Args{i.name, docId, score}
	if language != "" {
		args = args.Add("LANGUAGE", language)
	}

	if replace {
		args = args.Add("REPLACE")
	}
	return redis.String(conn.Do("FT.ADDHASH", args...))
}

// Returns a list of all existing indexes.
func (i *Client) List() ([]string, error) {
	conn := i.pool.Get()
	defer conn.Close()

	res, err := redis.Values(conn.Do("FT._LIST"))
	if err != nil {
		return nil, err
	}

	var indexes []string

	// Iterate over the values
	for ii := 0; ii < len(res); ii += 1 {
		key, _ := redis.String(res[ii], nil)
		indexes = append(indexes, key)
	}

	return indexes, nil
}

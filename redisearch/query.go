package redisearch

import (
	"math"

	"github.com/gomodule/redigo/redis"
)

// Flag is a type for query flags
type Flag uint64

// Query Flags
const (
	// Treat the terms verbatim and do not perform expansion
	QueryVerbatim Flag = 0x1

	// Do not load any content from the documents, return just IDs
	QueryNoContent Flag = 0x2

	// Fetch document scores as well as IDs and fields
	QueryWithScores Flag = 0x4

	// The query terms must appear in order in the document
	QueryInOrder Flag = 0x08

	// Fetch document payloads as well as fields. See documentation for payloads on redisearch.io
	QueryWithPayloads Flag = 0x10

	// ... more to come!

	DefaultOffset = 0
	DefaultNum    = 10
)

// SortingKey represents the sorting option if the query needs to be
// sorted based on a sortable fields and not a ranking function.
// See http://redisearch.io/Sorting/
type SortingKey struct {
	Field     string
	Ascending bool
}

func NewSortingKeyDir(field string, ascending bool) *SortingKey {
	return &SortingKey{
		Field:     field,
		Ascending: ascending,
	}
}

func (s SortingKey) Serialize() redis.Args {
	args := redis.Args{s.Field}
	if s.Ascending {
		args = args.Add("ASC")
	} else {
		args = args.Add("DESC")
	}
	return args
}

// HighlightOptions represents the options to higlight specific document fields.
// See http://redisearch.io/Highlight/
type HighlightOptions struct {
	Fields []string
	Tags   [2]string
}

// SummaryOptions represents the configuration used to create field summaries.
// See http://redisearch.io/Highlight/
type SummaryOptions struct {
	Fields       []string
	FragmentLen  int    // default 20
	NumFragments int    // default 3
	Separator    string // default "..."
}

// Query is a single search query and all its parameters and predicates
type Query struct {
	Raw string

	Paging Paging
	Flags  Flag
	Slop   *int

	Filters       []Filter
	InKeys        []string
	InFields      []string
	ReturnFields  []string
	Language      string
	Expander      string
	Scorer        string
	Payload       []byte
	SortBy        *SortingKey
	HighlightOpts *HighlightOptions
	SummarizeOpts *SummaryOptions
}

// Paging represents the offset paging of a search result
type Paging struct {
	Offset int
	Num    int
}

func NewPaging(offset int, num int) *Paging {
	return &Paging{
		Offset: offset,
		Num:    num,
	}
}

func (p Paging) serialize() redis.Args {
	args := redis.Args{}
	// only serialize something if it's different than the default
	// The default is 0 10
	// when either offset or num is default number, then need to set limit too
	if !(p.Offset == DefaultOffset && p.Num == DefaultNum) {
		args = args.Add("LIMIT", p.Offset, p.Num)
	}
	return args
}

// NewQuery creates a new query for a given index with the given search term.
// For currently the index parameter is ignored
func NewQuery(raw string) *Query {
	return &Query{
		Raw:     raw,
		Filters: []Filter{},
		Paging:  Paging{DefaultOffset, DefaultNum},
	}
}

func (q Query) serialize() redis.Args {

	args := redis.Args{q.Raw}.AddFlat(q.Paging.serialize())
	if q.Flags&QueryVerbatim != 0 {
		args = args.Add("VERBATIM")
	}

	if q.Flags&QueryNoContent != 0 {
		args = args.Add("NOCONTENT")
	}

	if q.Slop != nil {
		args = args.Add("SLOP", *q.Slop)
	}

	if q.Flags&QueryInOrder != 0 {
		args = args.Add("INORDER")
	}
	if q.Flags&QueryWithPayloads != 0 {
		args = args.Add("WITHPAYLOADS")
	}
	if q.Flags&QueryWithScores != 0 {
		args = args.Add("WITHSCORES")
	}

	if q.InKeys != nil {
		args = args.Add("INKEYS", len(q.InKeys))
		args = args.AddFlat(q.InKeys)
	}

	if q.InFields != nil {
		args = args.Add("INFIELDS", len(q.InFields))
		args = args.AddFlat(q.InFields)
	}

	if q.ReturnFields != nil {
		args = args.Add("RETURN", len(q.ReturnFields))
		args = args.AddFlat(q.ReturnFields)
	}

	if q.Scorer != "" {
		args = args.Add("SCORER", q.Scorer)
	}

	if q.Language != "" {
		args = args.Add("LANGUAGE", q.Language)
	}

	if q.Expander != "" {
		args = args.Add("EXPANDER", q.Expander)
	}

	if q.SortBy != nil {
		args = args.Add("SORTBY").AddFlat(q.SortBy.Serialize())
	}

	if q.HighlightOpts != nil {
		args = args.Add("HIGHLIGHT")
		if q.HighlightOpts.Fields != nil && len(q.HighlightOpts.Fields) > 0 {
			args = args.Add("FIELDS", len(q.HighlightOpts.Fields))
			args = args.AddFlat(q.HighlightOpts.Fields)
		}
		args = args.Add("TAGS", q.HighlightOpts.Tags[0], q.HighlightOpts.Tags[1])
	}

	if q.SummarizeOpts != nil {
		args = args.Add("SUMMARIZE")
		if q.SummarizeOpts.Fields != nil && len(q.SummarizeOpts.Fields) > 0 {
			args = args.Add("FIELDS", len(q.SummarizeOpts.Fields))
			args = args.AddFlat(q.SummarizeOpts.Fields)
		}
		if q.SummarizeOpts.FragmentLen > 0 {
			args = args.Add("LEN", q.SummarizeOpts.FragmentLen)
		}
		if q.SummarizeOpts.NumFragments > 0 {
			args = args.Add("FRAGS", q.SummarizeOpts.NumFragments)
		}
		if q.SummarizeOpts.Separator != "" {
			args = args.Add("SEPARATOR", q.SummarizeOpts.Separator)
		}
	}

	if q.Filters != nil {
		for _, f := range q.Filters {
			if f.Options != nil {
				switch f.Options.(type) {
				case NumericFilterOptions:
					opts, _ := f.Options.(NumericFilterOptions)
					args = append(args, "FILTER", f.Field)
					args = appendNumArgs(opts.Min, opts.ExclusiveMin, args)
					args = appendNumArgs(opts.Max, opts.ExclusiveMax, args)
				case GeoFilterOptions:
					opts, _ := f.Options.(GeoFilterOptions)
					args = append(args, "GEOFILTER", f.Field, opts.Lon, opts.Lat, opts.Radius, opts.Unit)
				}
			}
		}
	}
	return args
}

func appendNumArgs(num float64, exclude bool, args redis.Args) redis.Args {
	if math.IsInf(num, 1) {
		return append(args, "+inf")
	}
	if math.IsInf(num, -1) {
		return append(args, "-inf")
	}

	if exclude {
		return append(args, "(", num)
	}
	return append(args, num)
}

// AddFilter adds a filter to the query
func (q *Query) AddFilter(f Filter) *Query {
	if q.Filters == nil {
		q.Filters = []Filter{}
	}
	q.Filters = append(q.Filters, f)
	return q
}

// // AddPredicate adds a predicate to the query's filters
// func (q *Query) AddPredicate(p Predicate) *Query {
// 	q.Predicates = append(q.Predicates, p)
// 	return q
// }

// Limit sets the paging offset and limit for the query
// you can use LIMIT 0 0 to count the number of documents in the resultset without actually returning them
func (q *Query) Limit(offset, num int) *Query {
	q.Paging.Offset = offset
	q.Paging.Num = num
	return q
}

// SetFlags sets the query's optional flags
func (q *Query) SetFlags(flags Flag) *Query {
	q.Flags = flags
	return q
}

// SetInKeys sets the INKEYS argument of the query - limiting the search to a given set of IDs
func (q *Query) SetInKeys(keys ...string) *Query {
	q.InKeys = keys
	return q
}

// SetInFields sets the INFIELDS argument of the query - filter the results to ones appearing only in specific fields of the document
func (q *Query) SetInFields(fields ...string) *Query {
	q.InFields = fields
	return q
}

// SetSortBy sets the sorting key for the query
func (q *Query) SetSortBy(field string, ascending bool) *Query {
	q.SortBy = &SortingKey{Field: field, Ascending: ascending}
	return q
}

// SetReturnFields sets the fields that should be returned from each result.
// By default we return everything
func (q *Query) SetReturnFields(fields ...string) *Query {
	q.ReturnFields = fields
	return q
}

// AddReturnFields adds the fields that should be returned from each result
// to the ReturnFields property
func (q *Query) AddReturnFields(fields ...string) *Query {
	q.ReturnFields = append(q.ReturnFields, fields...)
	return q
}

// AddReturnField adds a single field with AS name that should be returned from
// each result to the ReturnFields property
func (q *Query) AddReturnField(field string, asName string) *Query {
	q.ReturnFields = append(q.ReturnFields, field, "AS", asName)
	return q
}

// SetPayload sets a binary payload to the query, that can be used by custom scoring functions
func (q *Query) SetPayload(payload []byte) *Query {
	q.Payload = payload
	return q
}

// SetLanguage sets the query language, used by the stemmer to expand the query
func (q *Query) SetLanguage(lang string) *Query {
	q.Language = lang
	return q
}

// SetScorer sets an alternative scoring function to be used.
// The only pre-compiled supported one at the moment is DISMAX
func (q *Query) SetScorer(scorer string) *Query {
	q.Scorer = scorer
	return q
}

// SetExpander sets a custom user query expander to be used
func (q *Query) SetExpander(exp string) *Query {
	q.Expander = exp
	return q
}

// Highlight sets highighting on given fields. Highlighting marks all the query terms
// with the given open and close tags (i.e. <b> and </b> for HTML)
func (q *Query) Highlight(fields []string, openTag, closeTag string) *Query {
	q.HighlightOpts = &HighlightOptions{
		Fields: fields,
		Tags:   [2]string{openTag, closeTag},
	}
	return q
}

// Summarize sets summarization on the given list of fields.
// It will instruct the engine to extract the most relevant snippets
// from the fields and return them as the field content.
// This function works with the default values of the engine, and only sets the fields.
// There is a function that accepts all options - SummarizeOptions
func (q *Query) Summarize(fields ...string) *Query {

	q.SummarizeOpts = &SummaryOptions{
		Fields: fields,
	}
	return q
}

// SummarizeOptions sets summarization on the given list of fields.
// It will instruct the engine to extract the most relevant snippets
// from the fields and return them as the field content.
//
// This function accepts advanced settings for snippet length, separators and number of snippets
func (q *Query) SummarizeOptions(opts SummaryOptions) *Query {
	q.SummarizeOpts = &opts
	return q
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
		args = SerializeIndexingOptions(opts, args)

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

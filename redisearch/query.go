package redisearch

import "github.com/garyburd/redigo/redis"

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
	Slop   int

	Filters       []Predicate
	InKeys        []string
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

// NewQuery creates a new query for a given index with the given search term.
// For currently the index parameter is ignored
func NewQuery(raw string) *Query {
	return &Query{
		Raw:     raw,
		Filters: []Predicate{},
		Paging:  Paging{DefaultOffset, DefaultNum},
	}
}

func (q Query) serialize() redis.Args {

	args := redis.Args{q.Raw, "LIMIT", q.Paging.Offset, q.Paging.Num}
	if q.Flags&QueryVerbatim != 0 {
		args = args.Add("VERBATIM")
	}

	if q.Flags&QueryNoContent != 0 {
		args = args.Add("NOCONTENT")
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

	if q.ReturnFields != nil {
		args = args.Add("RETURN", len(q.ReturnFields))
		args = args.AddFlat(q.ReturnFields)
	}

	if q.Scorer != "" {
		args = args.Add("SCORER", q.Scorer)
	}

	if q.Expander != "" {
		args = args.Add("EXPANDER", q.Expander)
	}

	if q.SortBy != nil {
		args = args.Add("SORTBY", q.SortBy.Field)
		if q.SortBy.Ascending {
			args = args.Add("ASC")
		} else {
			args = args.Add("DESC")
		}
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
	return args
}

// // AddPredicate adds a predicate to the query's filters
// func (q *Query) AddPredicate(p Predicate) *Query {
// 	q.Predicates = append(q.Predicates, p)
// 	return q
// }

// Limit sets the paging offset and limit for the query
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

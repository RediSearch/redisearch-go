package redisearch

import "github.com/garyburd/redigo/redis"

// Flag is a type for query flags
type Flag uint64

const (
	QueryVerbatim     Flag = 0x1
	QueryNoContent    Flag = 0x2
	QueryWithScores   Flag = 0x4
	QueryInOrder      Flag = 0x08
	QueryWithPayloads Flag = 0x10

	// ... more to come!

	DefaultOffset = 0
	DefaultNum    = 10
)

type SortingKey struct {
	Field     string
	Ascending bool
}

// Query is a single search query and all its parameters and predicates
type Query struct {
	Raw string

	Paging Paging
	Flags  Flag
	Slop   int

	Filters      []Predicate
	InKeys       []string
	ReturnFields []string
	Language     string
	Expander     string
	Scorer       string
	Payload      []byte
	SortBy       *SortingKey
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

func (q *Query) SetInKeys(keys ...string) *Query {
	q.InKeys = keys
	return q
}

func (q *Query) SetSortBy(field string, ascending bool) *Query {
	q.SortBy = &SortingKey{Field: field, Ascending: ascending}
	return q
}
func (q *Query) SetReturnFields(fields ...string) *Query {
	q.ReturnFields = fields
	return q
}

func (q *Query) SetPayload(payload []byte) *Query {
	q.Payload = payload
	return q
}

func (q *Query) SetLanguage(lang string) *Query {
	q.Language = lang
	return q
}

func (q *Query) SetScorer(scorer string) *Query {
	q.Scorer = scorer
	return q
}

func (q *Query) SetExpander(exp string) *Query {
	q.Expander = exp
	return q
}

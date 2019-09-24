package redisearch

import "github.com/garyburd/redigo/redis"

// GroupBy
type GroupBy struct {
	Fields   []string
	Reducers []GroupByReducers
}

func NewGroupBy(field string) *GroupBy {
	return &GroupBy{
		Fields:   []string{field},
		Reducers: make([]GroupByReducers, 0),
	}
}

func NewGroupByFields(fields []string) *GroupBy {
	return &GroupBy{
		Fields:   fields,
		Reducers: make([]GroupByReducers, 0),
	}
}

func (g *GroupBy) AddGroupByReducer(reducer GroupByReducers) *GroupBy {
	g.Reducers = append(g.Reducers, reducer)
	return g
}

// AggregateQuery
type AggregateQuery struct {
	Query      *Query
	WithSchema bool
	Verbatim   bool
	Max        int
	Groups     []GroupBy
}

func NewAggregateQuery(query *Query) *AggregateQuery {
	return &AggregateQuery{
		Query:      query,
		WithSchema: false,
		Verbatim:   false,
		Max:        0,
		Groups:     make([]GroupBy, 0),
	}
}

func (a *AggregateQuery) SetWithSchema(value bool) *AggregateQuery {
	a.WithSchema = value
	return a
}

func (a *AggregateQuery) SetVerbatim(value bool) *AggregateQuery {
	a.Verbatim = value
	return a
}

func (a *AggregateQuery) SetMax(value int) *AggregateQuery {
	a.Max = value
	return a
}

func (a *AggregateQuery) GroupBy(group GroupBy) *AggregateQuery {
	a.Groups = append(a.Groups, group)
	return a
}

func (q AggregateQuery) serialize() redis.Args {
	args := redis.Args{}
	args = args.Add(q.Query.serialize())
	if q.WithSchema {
		args = args.Add("WITHSCHEMA")
	}
	if q.Verbatim {
		args = args.Add("VERBATIM")
	}
	//TODO: LOAD logic
	//TODO: GROUPBY logic
	return args
}

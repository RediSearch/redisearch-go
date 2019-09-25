package redisearch

import "github.com/garyburd/redigo/redis"

// Projection
type Projection struct {
	Alias      string
	Expression string
}

func NewProjection(alias string, expression string) *Projection {
	return &Projection{
		Alias:      alias,
		Expression: expression,
	}
}

func (p Projection) Serialize() redis.Args {
	args := redis.Args{"APPLY", p.Expression, "AS", p.Alias}
	return args
}

// GroupBy
type GroupBy struct {
	Fields   []string
	Reducers []Reducer
	Paging   *Paging
}

func NewGroupBy(field string) *GroupBy {
	return &GroupBy{
		Fields:   []string{field},
		Reducers: make([]Reducer, 0),
		Paging:   nil,
	}
}

func NewGroupByFields(fields []string) *GroupBy {
	return &GroupBy{
		Fields:   fields,
		Reducers: make([]Reducer, 0),
		Paging:   nil,
	}
}

func (g *GroupBy) Reduce(reducer Reducer) *GroupBy {
	g.Reducers = append(g.Reducers, reducer)
	return g
}

func (g *GroupBy) Limit(offset int, num int) *GroupBy {
	g.Paging = NewPaging(offset, num)
	return g
}

func (g GroupBy) Serialize() redis.Args {
	ret := len(g.Fields)
	args := redis.Args{"GROUPBY", ret}.AddFlat(g.Fields)
	for _, reducer := range g.Reducers {
		args = args.AddFlat(reducer.Serialize())
	}
	if g.Paging != nil {
		args = args.AddFlat(g.Paging.serialize())
	}
	return args
}

// AggregateQuery
type AggregateQuery struct {
	Query            *Query
	Groups           []GroupBy
	Projections      []Projection
	Paging           *Paging
	SortByProperties []SortingKey
	Max              int
	WithSchema       bool
	Verbatim         bool
	Filters []string
	//Cursor

}

func NewAggregateQuery() *AggregateQuery {
	return &AggregateQuery{
		Query:            nil,
		Groups:           make([]GroupBy, 0),
		Projections:      make([]Projection, 0),
		Paging:           nil,
		SortByProperties: make([]SortingKey, 0),
		Max:              0,
		WithSchema:       false,
		Verbatim:         false,
		Filters: make([]string, 0),
		//Cursor:  make([]interface{}, 0),

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

//Specify one projection expression to add to each result
func (a *AggregateQuery) Apply(expression Projection) *AggregateQuery {
	a.Projections = append(a.Projections, expression)
	return a
}

//Sets the limit for the most recent group or query.
//If no group has been defined yet (via `GroupBy()`) then this sets
//the limit for the initial pool of results from the query. Otherwise,
//this limits the number of items operated on from the previous group.
//Setting a limit on the initial search results may be useful when
//attempting to execute an aggregation on a sample of a large data set.
func (a *AggregateQuery) Limit(offset int, num int) *AggregateQuery {
	ngroups := len(a.Groups)
	if ngroups > 0 {
		a.Groups[ngroups-1] = *a.Groups[ngroups-1].Limit(offset, num)
	} else {
		a.Paging = NewPaging(offset, num)
	}
	return a
}

func (a *AggregateQuery) GroupBy(group GroupBy) *AggregateQuery {
	a.Groups = append(a.Groups, group)
	return a
}

func (a *AggregateQuery) SortBy(sortby SortingKey) *AggregateQuery {
	a.SortByProperties = append(a.SortByProperties, sortby)
	return a
}

//Specify filters to filter the results using predicates relating to values in the result set.
func (a *AggregateQuery) Filter(expression string) *AggregateQuery {
	a.Filters = append(a.Filters, expression)
	return a
}

//Serialize order is defined as follows:
//{query_string:string}
//[WITHSCHEMA] [VERBATIM]
//[LOAD {nargs:integer} {property:string} ...]
//[GROUPBY
//{nargs:integer} {property:string} ...
//REDUCE
//{FUNC:string}
//{nargs:integer} {arg:string} ...
//[AS {name:string}]
//...
//] ...
//[SORTBY
//{nargs:integer} {string} ...
//[MAX {num:integer}] ...
//] ...
//[APPLY
//{EXPR:string}
//AS {name:string}
//] ...
//[FILTER {EXPR:string}] ...
//[LIMIT {offset:integer} {num:integer} ] ...

func (q AggregateQuery) Serialize() redis.Args {
	args := redis.Args{}
	if q.Query != nil {
		args = args.AddFlat(q.Query.serialize())
	} else {
		args = args.Add("*")
	}

	if q.WithSchema {
		args = args.Add("WITHSCHEMA")
	}
	if q.Verbatim {
		args = args.Add("VERBATIM")
	}

	// TODO: add cursor
	// TODO: add load fields

	for _, group := range q.Groups {
		args = args.AddFlat(group.Serialize())
	}
	for _, projector := range q.Projections {
		args = args.AddFlat(projector.Serialize())
	}
	nsort := len(q.SortByProperties)
	if nsort > 0 {
		args = args.Add("SORTBY", nsort*2)
		for _, sortby := range q.SortByProperties {
			args = args.AddFlat(sortby.Serialize())
		}
		if q.Max > 0 {
			args = args.Add("MAX", q.Max)
		}
	}
	for _, filter := range q.Filters {
		args = args.Add("FILTER",filter)
	}
	if q.Paging != nil {
		args = args.AddFlat(q.Paging.serialize())
	}

	return args
}

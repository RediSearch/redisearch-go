package redisearch

import (
	"fmt"
	"github.com/gomodule/redigo/redis"
	"log"
	"reflect"
)

// Projection
type Projection struct {
	Expression string
	Alias      string
}

func NewProjection(expression string, alias string) *Projection {
	return &Projection{
		Expression: expression,
		Alias:      alias,
	}
}

func (p Projection) Serialize() redis.Args {
	args := redis.Args{"APPLY", p.Expression, "AS", p.Alias}
	return args
}

// Cursor
type Cursor struct {
	Id      int
	Count   int
	MaxIdle int
}

func NewCursor() *Cursor {
	return &Cursor{
		Id:      0,
		Count:   0,
		MaxIdle: 0,
	}
}

func (c *Cursor) SetId(id int) *Cursor {
	c.Id = id
	return c
}

func (c *Cursor) SetCount(count int) *Cursor {
	c.Count = count
	return c
}

func (c *Cursor) SetMaxIdle(maxIdle int) *Cursor {
	c.MaxIdle = maxIdle
	return c
}

func (c Cursor) Serialize() redis.Args {
	args := redis.Args{"WITHCURSOR"}
	if c.Count > 0 {
		args = args.Add("COUNT", c.Count)
	}
	if c.MaxIdle > 0 {
		args = args.Add("MAXIDLE", c.MaxIdle)
	}
	return args
}

// GroupBy
type GroupBy struct {
	Fields   []string
	Reducers []Reducer
	Paging   *Paging
}

func NewGroupBy() *GroupBy {
	return &GroupBy{
		Fields:   make([]string, 0),
		Reducers: make([]Reducer, 0),
		Paging:   nil,
	}
}

func (g *GroupBy) AddFields(fields interface{}) *GroupBy {
	switch fields.(type) {
	case string:
		g.Fields = append(g.Fields, fields.(string))
	case []string:
		g.Fields = append(g.Fields, fields.([]string)...)
	default:
		return g
	}
	return g
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
	Query         *Query
	AggregatePlan redis.Args
	Paging        *Paging
	Max           int
	WithSchema    bool
	Verbatim      bool
	WithCursor    bool
	Cursor        *Cursor
	// TODO: add load fields

}

func NewAggregateQuery() *AggregateQuery {
	return &AggregateQuery{
		Query:      nil,
		Paging:     nil,
		Max:        0,
		WithSchema: false,
		Verbatim:   false,
		WithCursor: false,
	}
}

func (a *AggregateQuery) SetQuery(query *Query) *AggregateQuery {
	a.Query = query
	return a
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

func (a *AggregateQuery) SetCursor(cursor *Cursor) *AggregateQuery {
	a.WithCursor = true
	a.Cursor = cursor
	return a
}

func (a *AggregateQuery) CursorHasResults() (res bool) {
	res = false
	if !reflect.ValueOf(a.Cursor).IsNil() {
		res = a.Cursor.Id > 0
	}
	return
}

//Adds a APPLY clause to the aggregate plan
func (a *AggregateQuery) Apply(expression Projection) *AggregateQuery {
	a.AggregatePlan = a.AggregatePlan.AddFlat(expression.Serialize())
	return a
}

//Sets the limit for the initial pool of results from the query.
func (a *AggregateQuery) Limit(offset int, num int) *AggregateQuery {
	a.Paging = NewPaging(offset, num)
	return a
}

//Load document fields from the document HASH objects (if they are not in the sortables)
func (a *AggregateQuery) Load(Properties []string) *AggregateQuery {
	nproperties := len(Properties)
	if nproperties > 0 {
		a.AggregatePlan = a.AggregatePlan.Add("LOAD", nproperties)
		for _, property := range Properties {
			a.AggregatePlan = a.AggregatePlan.Add(fmt.Sprintf("@%s", property))
		}
	}
	return a
}

//Adds a GROUPBY clause to the aggregate plan
func (a *AggregateQuery) GroupBy(group GroupBy) *AggregateQuery {
	a.AggregatePlan = a.AggregatePlan.AddFlat(group.Serialize())
	return a
}

//Adds a SORTBY clause to the aggregate plan
func (a *AggregateQuery) SortBy(SortByProperties []SortingKey) *AggregateQuery {
	nsort := len(SortByProperties)
	if nsort > 0 {
		a.AggregatePlan = a.AggregatePlan.Add("SORTBY", nsort*2)
		for _, sortby := range SortByProperties {
			a.AggregatePlan = a.AggregatePlan.AddFlat(sortby.Serialize())
		}
		if a.Max > 0 {
			a.AggregatePlan = a.AggregatePlan.Add("MAX", a.Max)
		}
	}
	return a
}

//Specify filters to filter the results using predicates relating to values in the result set.
func (a *AggregateQuery) Filter(expression string) *AggregateQuery {
	a.AggregatePlan = a.AggregatePlan.Add("FILTER", expression)
	//a.Filters = append(a.Filters, expression)
	return a
}

func (q AggregateQuery) Serialize() redis.Args {
	args := redis.Args{}
	if q.Query != nil {
		args = args.AddFlat(q.Query.serialize())
	} else {
		args = args.Add("*")
	}
	// WITHSCHEMA
	if q.WithSchema {
		args = args.AddFlat("WITHSCHEMA")
	}
	// VERBATIM
	if q.Verbatim {
		args = args.Add("VERBATIM")
	}

	// WITHCURSOR
	if q.WithCursor {
		args = args.AddFlat(q.Cursor.Serialize())
	}

	// TODO: add load fields

	//Add the aggregation plan with ( GROUPBY and REDUCE | SORTBY | APPLY | FILTER ).+ clauses
	args = args.AddFlat(q.AggregatePlan)

	// LIMIT
	if !reflect.ValueOf(q.Paging).IsNil() {
		args = args.Add("LIMIT", q.Paging.Offset, q.Paging.Num)
	}

	return args
}

// Deprecated: Please use processAggReply() instead
func ProcessAggResponse(res []interface{}) [][]string {
	aggregateReply := make([][]string, len(res), len(res))
	for i := 0; i < len(res); i++ {
		if d, e := redis.Strings(res[i], nil); e == nil {
			aggregateReply[i] = d
		} else {
			log.Print("Error parsing Aggregate Reply: ", e)
			aggregateReply[i] = nil
		}
	}
	return aggregateReply
}

func processAggReply(res []interface{}) (total int, aggregateReply [][]string, err error) {
	aggregateReply = [][]string{}
	total = 0
	aggregate_results := len(res) - 1
	if aggregate_results > 0 {
		total = aggregate_results
		aggregateReply = make([][]string, aggregate_results, aggregate_results)
		for i := 0; i < aggregate_results; i++ {
			if d, e := redis.Strings(res[i+1], nil); e == nil {
				aggregateReply[i] = d
			} else {
				err = fmt.Errorf("Error parsing Aggregate Reply: %v on reply position %d", e, i)
				aggregateReply[i] = nil
			}
		}
	}
	return
}

func ProcessAggResponseSS(res []interface{}) [][]string {
	var lout = len(res)
	aggregateReply := make([][]string, lout, lout)
	for i := 0; i < lout; i++ {
		reply := res[i].([]interface{})
		linner := len(reply)
		aggregateReply[i] = make([]string, linner, linner)
		for j := 0; j < linner; j++ {
			if reply[j] == nil {
				log.Print(fmt.Sprintf("Error parsing Aggregate Reply on position (%d,%d)", i, j))
			} else {
				aggregateReply[i][j] = reply[j].(string)
			}

		}
	}
	return aggregateReply
}

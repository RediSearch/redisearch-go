package redisearch

import "github.com/gomodule/redigo/redis"

// GroupByReducers
type GroupByReducers string

const (
	// GroupByReducerCount is an alias for GROUPBY reducer COUNT
	GroupByReducerCount = GroupByReducers("COUNT")

	// GroupByReducerCountDistinct is an alias for GROUPBY reducer COUNT_DISTINCT
	GroupByReducerCountDistinct = GroupByReducers("COUNT_DISTINCT")

	// GroupByReducerCountDistinctish is an alias for GROUPBY reducer COUNT_DISTINCTISH
	GroupByReducerCountDistinctish = GroupByReducers("COUNT_DISTINCTISH")

	// GroupByReducerSum is an alias for GROUPBY reducer SUM
	GroupByReducerSum = GroupByReducers("SUM")

	// GroupByReducerMin is an alias for GROUPBY reducer MIN
	GroupByReducerMin = GroupByReducers("MIN")

	// GroupByReducerMax is an alias for GROUPBY reducer MAX
	GroupByReducerMax = GroupByReducers("MAX")

	// GroupByReducerAvg is an alias for GROUPBY reducer AVG
	GroupByReducerAvg = GroupByReducers("AVG")

	// GroupByReducerStdDev is an alias for GROUPBY reducer STDDEV
	GroupByReducerStdDev = GroupByReducers("STDDEV")

	// GroupByReducerQuantile is an alias for GROUPBY reducer QUANTILE
	GroupByReducerQuantile = GroupByReducers("QUANTILE")

	// GroupByReducerToList is an alias for GROUPBY reducer TOLIST
	GroupByReducerToList = GroupByReducers("TOLIST")

	// GroupByReducerFirstValue is an alias for GROUPBY reducer FIRST_VALUE
	GroupByReducerFirstValue = GroupByReducers("FIRST_VALUE")

	// GroupByReducerRandomSample is an alias for GROUPBY reducer RANDOM_SAMPLE
	GroupByReducerRandomSample = GroupByReducers("RANDOM_SAMPLE")
)

// Reducer represents an index schema Schema, or how the index would
// treat documents sent to it.
type Reducer struct {
	Name  GroupByReducers
	Alias string
	Args  []string
}

// NewReducer creates a new Reducer object
func NewReducer(name GroupByReducers, args []string) *Reducer {
	return &Reducer{
		Name:  name,
		Alias: "",
		Args:  args,
	}
}

// NewReducer creates a new Reducer object
func NewReducerAlias(name GroupByReducers, args []string, alias string) *Reducer {
	return &Reducer{
		Name:  name,
		Alias: alias,
		Args:  args,
	}
}

func (r *Reducer) SetName(reducer GroupByReducers) *Reducer {
	r.Name = reducer
	return r
}

func (r *Reducer) SetArgs(args []string) *Reducer {
	r.Args = args
	return r
}

func (r *Reducer) SetAlias(a string) *Reducer {
	r.Alias = a
	return r
}

func (r Reducer) Serialize() redis.Args {
	ret := len(r.Args)
	args := redis.Args{"REDUCE", r.Name, ret}.AddFlat(r.Args)
	if r.Alias != "" {
		args = append(args, "AS", r.Alias)
	}
	return args
}

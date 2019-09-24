package redisearch

// Sort direction
type DirString string

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

	// SortDirectionAsc is an alias for Sort Direction ascending
	SortDirectionAsc = DirString("ASC")

	// SortDirectionDesc is an alias for Sort Direction descending
	SortDirectionDesc = DirString("DESC")
)

// Reducer represents an index schema Schema, or how the index would
// treat documents sent to it.
type Reducer struct {
	Name          GroupByReducers
	Alias         string
	SortDirection DirString
}

// NewReducer creates a new Reducer object
func NewReducer(name GroupByReducers) *Reducer {
	return &Reducer{
		Name:          name,
		SortDirection: SortDirectionAsc,
	}
}

func (r *Reducer) SetName(reducer GroupByReducers) *Reducer {
	r.Name = reducer
	return r
}

func (r *Reducer) SetAlias(a string) *Reducer {
	r.Alias = a
	return r
}

func (r *Reducer) SetSortDirection(dir DirString) *Reducer {
	r.SortDirection = dir
	return r
}

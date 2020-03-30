package redisearch_test

import (
	"bufio"
	"compress/bzip2"
	"encoding/json"
	"fmt"
	"github.com/RediSearch/redisearch-go/redisearch"
	"github.com/gomodule/redigo/redis"
	"github.com/stretchr/testify/assert"
	"log"
	"math/rand"
	"os"
	"reflect"
	"strconv"
	"strings"
	"testing"
)

// Game struct which contains a Asin, a Description, a Title, a Price, and a list of categories
// a type and a list of social links

// {"asin": "0984529527", "description": null, "title": "Dark Age Apocalypse: Forcelists HC", "brand": "Dark Age Miniatures", "price": 31.23, "categories": ["Games", "PC", "Video Games"]}
type Game struct {
	Asin        string   `json:"asin"`
	Description string   `json:"description"`
	Title       string   `json:"title"`
	Brand       string   `json:"brand"`
	Price       float32  `json:"price"`
	Categories  []string `json:"categories"`
}

func AddValues(c *redisearch.Client) {
	// Open our jsonFile
	bzipfile := "../tests/games.json.bz2"

	f, err := os.OpenFile(bzipfile, 0, 0)
	if err != nil {
		panic(err)
	}
	defer f.Close()
	// create a reader
	br := bufio.NewReader(f)
	// create a bzip2.reader, using the reader we just created
	cr := bzip2.NewReader(br)
	// create a reader, using the bzip2.reader we were passed
	d := bufio.NewReader(cr)
	// create a scanner
	scanner := bufio.NewScanner(d)
	docs := make([]redisearch.Document, 0)
	docPos := 1
	for scanner.Scan() {
		// we initialize our Users array
		var game Game

		err := json.Unmarshal(scanner.Bytes(), &game)
		if err != nil {
			fmt.Println("error:", err)
		}
		docs = append(docs, redisearch.NewDocument(fmt.Sprintf("docs-games-%d", docPos), 1).
			Set("title", game.Title).
			Set("brand", game.Brand).
			Set("description", game.Description).
			Set("price", game.Price).
			Set("categories", strings.Join(game.Categories, ",")))
		docPos = docPos + 1
	}

	if err := c.IndexOptions(redisearch.DefaultIndexingOptions, docs...); err != nil {
		log.Fatal(err)
	}

}
func init() {
	/* load test data */
	c := createClient("docs-games-idx1")

	sc := redisearch.NewSchema(redisearch.DefaultOptions).
		AddField(redisearch.NewTextFieldOptions("title", redisearch.TextFieldOptions{Sortable: true})).
		AddField(redisearch.NewTextFieldOptions("brand", redisearch.TextFieldOptions{Sortable: true, NoStem: true})).
		AddField(redisearch.NewTextField("description")).
		AddField(redisearch.NewSortableNumericField("price")).
		AddField(redisearch.NewTagField("categories"))

	c.Drop()
	c.CreateIndex(sc)

	AddValues(c)
}
func TestAggregateGroupBy(t *testing.T) {

	c := createClient("docs-games-idx1")

	q1 := redisearch.NewAggregateQuery().
		GroupBy(*redisearch.NewGroupBy().AddFields("@brand").
			Reduce(*redisearch.NewReducerAlias(redisearch.GroupByReducerCount, []string{}, "count"))).
		SortBy([]redisearch.SortingKey{*redisearch.NewSortingKeyDir("@count", false)}).
		Limit(0, 5)

	_, count, err := c.Aggregate(q1)
	assert.Nil(t, err)
	assert.Equal(t, 5, count)
}

func TestAggregateMinMax(t *testing.T) {

	c := createClient("docs-games-idx1")

	q1 := redisearch.NewAggregateQuery().SetQuery(redisearch.NewQuery("sony")).
		GroupBy(*redisearch.NewGroupBy().AddFields("@brand").
			Reduce(*redisearch.NewReducer(redisearch.GroupByReducerCount, []string{})).
			Reduce(*redisearch.NewReducerAlias(redisearch.GroupByReducerMin, []string{"@price"}, "minPrice"))).
		SortBy([]redisearch.SortingKey{*redisearch.NewSortingKeyDir("@minPrice", false)})

	res, _, err := c.Aggregate(q1)
	assert.Nil(t, err)
	row := res[0]
	f, _ := strconv.ParseFloat(row[5], 64)
	assert.GreaterOrEqual(t, f, 88.0)
	assert.Less(t, f, 89.0)

	q2 := redisearch.NewAggregateQuery().SetQuery(redisearch.NewQuery("sony")).
		GroupBy(*redisearch.NewGroupBy().AddFields("@brand").
			Reduce(*redisearch.NewReducer(redisearch.GroupByReducerCount, []string{})).
			Reduce(*redisearch.NewReducerAlias(redisearch.GroupByReducerMax, []string{"@price"}, "maxPrice"))).
		SortBy([]redisearch.SortingKey{*redisearch.NewSortingKeyDir("@maxPrice", false)})

	res, _, err = c.Aggregate(q2)
	assert.Nil(t, err)
	row = res[0]
	f, _ = strconv.ParseFloat(row[5], 64)
	assert.GreaterOrEqual(t, f, 695.0)
	assert.Less(t, f, 696.0)
}

func TestAggregateCountDistinct(t *testing.T) {

	c := createClient("docs-games-idx1")

	q1 := redisearch.NewAggregateQuery().
		GroupBy(*redisearch.NewGroupBy().AddFields("@brand").
			Reduce(*redisearch.NewReducer(redisearch.GroupByReducerCountDistinct, []string{"@title"}).SetAlias("count_distinct(title)")).
			Reduce(*redisearch.NewReducer(redisearch.GroupByReducerCount, []string{})))

	res, _, err := c.Aggregate(q1)
	assert.Nil(t, err)
	row := res[0]
	assert.Equal(t, "1484", row[3])
}

func TestAggregateFilter(t *testing.T) {

	c := createClient("docs-games-idx1")

	q1 := redisearch.NewAggregateQuery().
		GroupBy(*redisearch.NewGroupBy().AddFields("@brand").
			Reduce(*redisearch.NewReducerAlias(redisearch.GroupByReducerCount, []string{}, "count"))).
		Filter("@count > 5")

	res, _, err := c.Aggregate(q1)
	assert.Nil(t, err)
	for _, row := range res {
		f, _ := strconv.ParseFloat(row[3], 64)
		assert.Greater(t, f, 5.0)
	}

}

func makeAggResponseInterface(seed int64, nElements int, responseSizes []int) (res []interface{}) {
	rand.Seed(seed)
	nInner := len(responseSizes)
	s := make([]interface{}, nElements)
	for i := 0; i < nElements; i++ {
		sIn := make([]interface{}, nInner)
		for pos, elementSize := range responseSizes {
			token := make([]byte, elementSize)
			rand.Read(token)
			sIn[pos] = string(token)
		}
		s[i] = sIn
	}
	return s
}

func benchmarkProcessAggResponseSS(res []interface{}, total int, b *testing.B) {
	for n := 0; n < b.N; n++ {
		redisearch.ProcessAggResponseSS(res)
	}
}

func benchmarkProcessAggResponse(res []interface{}, total int, b *testing.B) {
	for n := 0; n < b.N; n++ {
		redisearch.ProcessAggResponse(res)
	}
}

func BenchmarkProcessAggResponse_10x4Elements(b *testing.B) {
	res := makeAggResponseInterface(12345, 10, []int{4, 20, 20, 4})
	b.ResetTimer()
	benchmarkProcessAggResponse(res, 10, b)
}

func BenchmarkProcessAggResponseSS_10x4Elements(b *testing.B) {
	res := makeAggResponseInterface(12345, 10, []int{4, 20, 20, 4})
	b.ResetTimer()
	benchmarkProcessAggResponseSS(res, 10, b)
}

func BenchmarkProcessAggResponse_100x4Elements(b *testing.B) {
	res := makeAggResponseInterface(12345, 100, []int{4, 20, 20, 4})
	b.ResetTimer()
	benchmarkProcessAggResponse(res, 100, b)
}

func BenchmarkProcessAggResponseSS_100x4Elements(b *testing.B) {
	res := makeAggResponseInterface(12345, 100, []int{4, 20, 20, 4})
	b.ResetTimer()
	benchmarkProcessAggResponseSS(res, 100, b)
}

func TestProjection_Serialize(t *testing.T) {
	type fields struct {
		Expression string
		Alias      string
	}
	tests := []struct {
		name   string
		fields fields
		want   redis.Args
	}{
		{"Test_Serialize_1", fields{"sqrt(log(foo) * floor(@bar/baz)) + (3^@qaz % 6)", "sqrt"}, redis.Args{"APPLY", "sqrt(log(foo) * floor(@bar/baz)) + (3^@qaz % 6)", "AS", "sqrt"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := redisearch.Projection{
				Expression: tt.fields.Expression,
				Alias:      tt.fields.Alias,
			}
			if got := p.Serialize(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("serialize() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestCursor_Serialize(t *testing.T) {
	type fields struct {
		Id      int
		Count   int
		MaxIdle int
	}
	tests := []struct {
		name   string
		fields fields
		want   redis.Args
	}{
		{"TestCursor_Serialize_1", fields{1, 0, 0,}, redis.Args{"WITHCURSOR"}},
		{"TestCursor_Serialize_2_MAXIDLE", fields{1, 0, 30000,}, redis.Args{"WITHCURSOR", "MAXIDLE", 30000}},
		{"TestCursor_Serialize_3_COUNT_MAXIDLE", fields{1, 10, 30000,}, redis.Args{"WITHCURSOR", "COUNT", 10, "MAXIDLE", 30000}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := redisearch.Cursor{
				Id:      tt.fields.Id,
				Count:   tt.fields.Count,
				MaxIdle: tt.fields.MaxIdle,
			}
			if got := c.Serialize(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("serialize() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGroupBy_AddFields(t *testing.T) {
	type fields struct {
		Fields   []string
		Reducers []redisearch.Reducer
		Paging   *redisearch.Paging
	}
	type args struct {
		fields interface{}
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *redisearch.GroupBy
	}{
		{"TestGroupBy_AddFields_1",
			fields{[]string{}, nil, nil},
			args{"a",},
			&redisearch.GroupBy{[]string{"a"}, nil, nil},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g := &redisearch.GroupBy{
				Fields:   tt.fields.Fields,
				Reducers: tt.fields.Reducers,
				Paging:   tt.fields.Paging,
			}
			if got := g.AddFields(tt.args.fields); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("AddFields() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGroupBy_Limit(t *testing.T) {
	type fields struct {
		Fields   []string
		Reducers []redisearch.Reducer
		Paging   *redisearch.Paging
	}
	type args struct {
		offset int
		num    int
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *redisearch.GroupBy
	}{
		{"TestGroupBy_Limit_1",
			fields{[]string{}, nil, nil},
			args{10, 20},
			&redisearch.GroupBy{[]string{}, nil, &redisearch.Paging{10, 20}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g := &redisearch.GroupBy{
				Fields:   tt.fields.Fields,
				Reducers: tt.fields.Reducers,
				Paging:   tt.fields.Paging,
			}
			if got := g.Limit(tt.args.offset, tt.args.num); (got.Paging.Num != tt.want.Paging.Num) || (got.Paging.Offset != tt.want.Paging.Offset) {
				t.Errorf("Limit() = %v, want %v, %v, want %v",
					got.Paging.Num, tt.want.Paging.Num,
					got.Paging.Offset, tt.want.Paging.Offset)
			}
		})
	}
}

func TestAggregateQuery_SetMax(t *testing.T) {
	type fields struct {
		Query         *redisearch.Query
		AggregatePlan redis.Args
		Paging        *redisearch.Paging
		Max           int
		WithSchema    bool
		Verbatim      bool
		WithCursor    bool
		Cursor        *redisearch.Cursor
	}
	type args struct {
		value int
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *redisearch.AggregateQuery
	}{
		{"TestAggregateQuery_SetMax_1",
			fields{nil, redis.Args{}, nil, 0, false, false, false, nil},
			args{10},
			&redisearch.AggregateQuery{nil, redis.Args{}, nil, 10, false, false, false, nil},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := &redisearch.AggregateQuery{
				Query:         tt.fields.Query,
				AggregatePlan: tt.fields.AggregatePlan,
				Paging:        tt.fields.Paging,
				Max:           tt.fields.Max,
				WithSchema:    tt.fields.WithSchema,
				Verbatim:      tt.fields.Verbatim,
				WithCursor:    tt.fields.WithCursor,
				Cursor:        tt.fields.Cursor,
			}
			if got := a.SetMax(tt.args.value); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("SetMax() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestAggregateQuery_SetVerbatim(t *testing.T) {
	type fields struct {
		Query         *redisearch.Query
		AggregatePlan redis.Args
		Paging        *redisearch.Paging
		Max           int
		WithSchema    bool
		Verbatim      bool
		WithCursor    bool
		Cursor        *redisearch.Cursor
	}
	type args struct {
		value bool
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *redisearch.AggregateQuery
	}{
		{"TestAggregateQuery_SetVerbatim_1",
			fields{nil, redis.Args{}, nil, 0, false, false, false, nil},
			args{true},
			&redisearch.AggregateQuery{nil, redis.Args{}, nil, 0, false, true, false, nil},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := &redisearch.AggregateQuery{
				Query:         tt.fields.Query,
				AggregatePlan: tt.fields.AggregatePlan,
				Paging:        tt.fields.Paging,
				Max:           tt.fields.Max,
				WithSchema:    tt.fields.WithSchema,
				Verbatim:      tt.fields.Verbatim,
				WithCursor:    tt.fields.WithCursor,
				Cursor:        tt.fields.Cursor,
			}
			if got := a.SetVerbatim(tt.args.value); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("SetVerbatim() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestAggregateQuery_SetWithSchema(t *testing.T) {
	type fields struct {
		Query         *redisearch.Query
		AggregatePlan redis.Args
		Paging        *redisearch.Paging
		Max           int
		WithSchema    bool
		Verbatim      bool
		WithCursor    bool
		Cursor        *redisearch.Cursor
	}
	type args struct {
		value bool
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *redisearch.AggregateQuery
	}{
		{"TestAggregateQuery_SetWithSchema_1",
			fields{nil, redis.Args{}, nil, 0, false, false, false, nil},
			args{true},
			&redisearch.AggregateQuery{nil, redis.Args{}, nil, 0, true, false, false, nil},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := &redisearch.AggregateQuery{
				Query:         tt.fields.Query,
				AggregatePlan: tt.fields.AggregatePlan,
				Paging:        tt.fields.Paging,
				Max:           tt.fields.Max,
				WithSchema:    tt.fields.WithSchema,
				Verbatim:      tt.fields.Verbatim,
				WithCursor:    tt.fields.WithCursor,
				Cursor:        tt.fields.Cursor,
			}
			if got := a.SetWithSchema(tt.args.value); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("SetWithSchema() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestAggregateQuery_CursorHasResults(t *testing.T) {
	type fields struct {
		Query         *redisearch.Query
		AggregatePlan redis.Args
		Paging        *redisearch.Paging
		Max           int
		WithSchema    bool
		Verbatim      bool
		WithCursor    bool
		Cursor        *redisearch.Cursor
	}
	tests := []struct {
		name    string
		fields  fields
		wantRes bool
	}{
		{"TestAggregateQuery_CursorHasResults_1_false",
			fields{nil, redis.Args{}, nil, 0, false, false, false, nil},
			false,
		},
		{"TestAggregateQuery_CursorHasResults_1_true",
			fields{nil, redis.Args{}, nil, 0, false, false, false, redisearch.NewCursor().SetId(10)},
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := &redisearch.AggregateQuery{
				Query:         tt.fields.Query,
				AggregatePlan: tt.fields.AggregatePlan,
				Paging:        tt.fields.Paging,
				Max:           tt.fields.Max,
				WithSchema:    tt.fields.WithSchema,
				Verbatim:      tt.fields.Verbatim,
				WithCursor:    tt.fields.WithCursor,
				Cursor:        tt.fields.Cursor,
			}
			if gotRes := a.CursorHasResults(); gotRes != tt.wantRes {
				t.Errorf("CursorHasResults() = %v, want %v", gotRes, tt.wantRes)
			}
		})
	}
}

func TestAggregateQuery_Load(t *testing.T) {
	type fields struct {
		Query         *redisearch.Query
		AggregatePlan redis.Args
		Paging        *redisearch.Paging
		Max           int
		WithSchema    bool
		Verbatim      bool
		WithCursor    bool
		Cursor        *redisearch.Cursor
	}
	type args struct {
		Properties []string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   redis.Args
	}{
		{"TestAggregateQuery_Load_1",
			fields{nil, redis.Args{}, nil, 0, false, false, false, nil},
			args{[]string{"field1"}},
			redis.Args{"*", "LOAD", 1, "@field1"},
		},
		{"TestAggregateQuery_Load_2",
			fields{nil, redis.Args{}, nil, 0, false, false, false, nil},
			args{[]string{"field1", "field2", "field3", "field4"}},
			redis.Args{"*", "LOAD", 4, "@field1", "@field2", "@field3", "@field4"},
		},
		{"TestAggregateQuery_Load_Empty",
			fields{nil, redis.Args{}, nil, 0, false, false, false, nil},
			args{[]string{}},
			redis.Args{"*"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := &redisearch.AggregateQuery{
				Query:         tt.fields.Query,
				AggregatePlan: tt.fields.AggregatePlan,
				Paging:        tt.fields.Paging,
				Max:           tt.fields.Max,
				WithSchema:    tt.fields.WithSchema,
				Verbatim:      tt.fields.Verbatim,
				WithCursor:    tt.fields.WithCursor,
				Cursor:        tt.fields.Cursor,
			}
			if got := a.Load(tt.args.Properties).Serialize(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Load() = %v, want %v", got, tt.want)
			}
		})
	}
}

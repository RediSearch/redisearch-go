package redisearch

import (
	"bufio"
	"compress/bzip2"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"os"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/gomodule/redigo/redis"
	"github.com/stretchr/testify/assert"
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

func init() {
	/* load test data */
	value, exists := os.LookupEnv("REDISEARCH_RDB_LOADED")
	requiresDatagen := true
	if exists && value != "" {
		requiresDatagen = false
	}
	if requiresDatagen {
		c := createClient("bench.ft.aggregate")

		sc := NewSchema(DefaultOptions).
			AddField(NewTextField("foo"))
		c.Drop()
		if err := c.CreateIndex(sc); err != nil {
			log.Fatal(err)
		}
		ndocs := 10000
		docs := make([]Document, ndocs)
		for i := 0; i < ndocs; i++ {
			docs[i] = NewDocument(fmt.Sprintf("bench.ft.aggregate.doc%d", i), 1).Set("foo", "hello world")
		}

		if err := c.IndexOptions(DefaultIndexingOptions, docs...); err != nil {
			log.Fatal(err)
		}
	}

}

func benchmarkAggregate(c *Client, q *AggregateQuery, b *testing.B) {
	for n := 0; n < b.N; n++ {
		c.Aggregate(q)
	}
}

func benchmarkAggregateCursor(c *Client, q *AggregateQuery, b *testing.B) {
	for n := 0; n < b.N; n++ {
		c.Aggregate(q)
		for q.CursorHasResults() {
			c.Aggregate(q)
		}
	}
}

func BenchmarkAgg_1(b *testing.B) {
	c := createClient("bench.ft.aggregate")
	q := NewAggregateQuery().
		SetQuery(NewQuery("*"))
	b.ResetTimer()
	benchmarkAggregate(c, q, b)
}

func BenchmarkAggCursor_1(b *testing.B) {
	c := createClient("bench.ft.aggregate")
	q := NewAggregateQuery().
		SetQuery(NewQuery("*")).
		SetCursor(NewCursor())
	b.ResetTimer()
	benchmarkAggregateCursor(c, q, b)
}

func AddValues(c *Client) {
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
	docs := make([]Document, 0)
	docPos := 1
	for scanner.Scan() {
		// we initialize our Users array
		var game Game

		err := json.Unmarshal(scanner.Bytes(), &game)
		if err != nil {
			fmt.Println("error:", err)
		}
		docs = append(docs, NewDocument(fmt.Sprintf("docs-games-%d", docPos), 1).
			Set("title", game.Title).
			Set("brand", game.Brand).
			Set("description", game.Description).
			Set("price", game.Price).
			Set("categories", strings.Join(game.Categories, ",")))
		docPos = docPos + 1
	}

	if err := c.IndexOptions(DefaultIndexingOptions, docs...); err != nil {
		log.Fatal(err)
	}

}
func Init() {
	/* load test data */
	c := createClient("docs-games-idx1")

	sc := NewSchema(DefaultOptions).
		AddField(NewTextFieldOptions("title", TextFieldOptions{Sortable: true})).
		AddField(NewTextFieldOptions("brand", TextFieldOptions{Sortable: true, NoStem: true})).
		AddField(NewTextField("description")).
		AddField(NewSortableNumericField("price")).
		AddField(NewTagField("categories"))

	c.Drop()
	c.CreateIndex(sc)

	AddValues(c)
}
func TestAggregateGroupBy(t *testing.T) {
	Init()
	c := createClient("docs-games-idx1")

	q1 := NewAggregateQuery().
		GroupBy(*NewGroupBy().AddFields("@brand").
			Reduce(*NewReducerAlias(GroupByReducerCount, []string{}, "count"))).
		SortBy([]SortingKey{*NewSortingKeyDir("@count", false)}).
		Limit(0, 5)

	_, count, err := c.Aggregate(q1)
	assert.Nil(t, err)
	assert.Equal(t, 5, count)
}

func TestAggregateMinMax(t *testing.T) {
	Init()
	c := createClient("docs-games-idx1")

	q1 := NewAggregateQuery().SetQuery(NewQuery("sony")).
		GroupBy(*NewGroupBy().AddFields("@brand").
			Reduce(*NewReducer(GroupByReducerCount, []string{})).
			Reduce(*NewReducerAlias(GroupByReducerMin, []string{"@price"}, "minPrice"))).
		SortBy([]SortingKey{*NewSortingKeyDir("@minPrice", false)})

	res, _, err := c.Aggregate(q1)
	assert.Nil(t, err)
	row := res[0]
	fmt.Println(row)
	f, _ := strconv.ParseFloat(row[5], 64)
	assert.GreaterOrEqual(t, f, 88.0)
	assert.Less(t, f, 89.0)

	q2 := NewAggregateQuery().SetQuery(NewQuery("sony")).
		GroupBy(*NewGroupBy().AddFields("@brand").
			Reduce(*NewReducer(GroupByReducerCount, []string{})).
			Reduce(*NewReducerAlias(GroupByReducerMax, []string{"@price"}, "maxPrice"))).
		SortBy([]SortingKey{*NewSortingKeyDir("@maxPrice", false)})

	res, _, err = c.Aggregate(q2)
	assert.Nil(t, err)
	row = res[0]
	f, _ = strconv.ParseFloat(row[5], 64)
	assert.GreaterOrEqual(t, f, 695.0)
	assert.Less(t, f, 696.0)
}

func TestAggregateCountDistinct(t *testing.T) {
	Init()
	c := createClient("docs-games-idx1")

	q1 := NewAggregateQuery().
		GroupBy(*NewGroupBy().AddFields("@brand").
			Reduce(*NewReducer(GroupByReducerCountDistinct, []string{"@title"}).SetAlias("count_distinct(title)")).
			Reduce(*NewReducer(GroupByReducerCount, []string{})))

	res, _, err := c.Aggregate(q1)
	assert.Nil(t, err)
	row := res[0]
	assert.Equal(t, "1484", row[3])
}

func TestAggregateFilter(t *testing.T) {
	Init()
	c := createClient("docs-games-idx1")

	q1 := NewAggregateQuery().
		GroupBy(*NewGroupBy().AddFields("@brand").
			Reduce(*NewReducerAlias(GroupByReducerCount, []string{}, "count"))).
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
		ProcessAggResponseSS(res)
	}
}

func benchmarkProcessAggResponse(res []interface{}, total int, b *testing.B) {
	for n := 0; n < b.N; n++ {
		ProcessAggResponse(res)
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
			p := Projection{
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
		{"TestCursor_Serialize_1", fields{1, 0, 0}, redis.Args{"WITHCURSOR"}},
		{"TestCursor_Serialize_2_MAXIDLE", fields{1, 0, 30000}, redis.Args{"WITHCURSOR", "MAXIDLE", 30000}},
		{"TestCursor_Serialize_3_COUNT_MAXIDLE", fields{1, 10, 30000}, redis.Args{"WITHCURSOR", "COUNT", 10, "MAXIDLE", 30000}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := Cursor{
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
		Reducers []Reducer
		Paging   *Paging
	}
	type args struct {
		fields interface{}
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *GroupBy
	}{
		{"TestGroupBy_AddFields_1",
			fields{[]string{}, nil, nil},
			args{"a"},
			&GroupBy{[]string{"a"}, nil, nil},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g := &GroupBy{
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
		Reducers []Reducer
		Paging   *Paging
	}
	type args struct {
		offset int
		num    int
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *GroupBy
	}{
		{"TestGroupBy_Limit_1",
			fields{[]string{}, nil, nil},
			args{10, 20},
			&GroupBy{[]string{}, nil, &Paging{10, 20}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g := &GroupBy{
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
		Query         *Query
		AggregatePlan redis.Args
		Paging        *Paging
		Max           int
		WithSchema    bool
		Verbatim      bool
		WithCursor    bool
		Cursor        *Cursor
	}
	type args struct {
		value int
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *AggregateQuery
	}{
		{"TestAggregateQuery_SetMax_1",
			fields{nil, redis.Args{}, nil, 0, false, false, false, nil},
			args{10},
			&AggregateQuery{nil, redis.Args{}, nil, 10, false, false, false, nil},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := &AggregateQuery{
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
		Query         *Query
		AggregatePlan redis.Args
		Paging        *Paging
		Max           int
		WithSchema    bool
		Verbatim      bool
		WithCursor    bool
		Cursor        *Cursor
	}
	type args struct {
		value bool
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *AggregateQuery
	}{
		{"TestAggregateQuery_SetVerbatim_1",
			fields{nil, redis.Args{}, nil, 0, false, false, false, nil},
			args{true},
			&AggregateQuery{nil, redis.Args{}, nil, 0, false, true, false, nil},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := &AggregateQuery{
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
		Query         *Query
		AggregatePlan redis.Args
		Paging        *Paging
		Max           int
		WithSchema    bool
		Verbatim      bool
		WithCursor    bool
		Cursor        *Cursor
	}
	type args struct {
		value bool
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *AggregateQuery
	}{
		{"TestAggregateQuery_SetWithSchema_1",
			fields{nil, redis.Args{}, nil, 0, false, false, false, nil},
			args{true},
			&AggregateQuery{nil, redis.Args{}, nil, 0, true, false, false, nil},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := &AggregateQuery{
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
		Query         *Query
		AggregatePlan redis.Args
		Paging        *Paging
		Max           int
		WithSchema    bool
		Verbatim      bool
		WithCursor    bool
		Cursor        *Cursor
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
			fields{nil, redis.Args{}, nil, 0, false, false, false, NewCursor().SetId(10)},
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := &AggregateQuery{
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
		Query         *Query
		AggregatePlan redis.Args
		Paging        *Paging
		Max           int
		WithSchema    bool
		Verbatim      bool
		WithCursor    bool
		Cursor        *Cursor
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
		{"TestAggregateQuery_Load_All",
			fields{nil, redis.Args{}, nil, 0, false, false, false, nil},
			args{[]string{}},
			redis.Args{"*", "LOAD", "*"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := &AggregateQuery{
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

func TestProcessAggResponse(t *testing.T) {
	type args struct {
		res []interface{}
	}
	tests := []struct {
		name string
		args args
		want [][]string
	}{
		{"empty-reply", args{[]interface{}{}}, [][]string{}},
		{"1-element-reply", args{[]interface{}{[]interface{}{"userFullName", "berge, julius", "count", "2783"}}}, [][]string{{"userFullName", "berge, julius", "count", "2783"}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ProcessAggResponse(tt.args.res); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ProcessAggResponse() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_processAggReply(t *testing.T) {
	type args struct {
		res []interface{}
	}
	tests := []struct {
		name               string
		args               args
		wantTotal          int
		wantAggregateReply [][]string
		wantErr            bool
	}{
		{"empty-reply", args{[]interface{}{}}, 0, [][]string{}, false},
		{"1-element-reply", args{[]interface{}{1, []interface{}{"userFullName", "j", "count", "2"}}}, 1, [][]string{{"userFullName", "j", "count", "2"}}, false},
		{"multi-element-reply", args{[]interface{}{2, []interface{}{"userFullName", "j"}, []interface{}{"userFullName", "a"}}}, 2, [][]string{{"userFullName", "j"}, {"userFullName", "a"}}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotTotal, gotAggregateReply, err := processAggReply(tt.args.res)
			if (err != nil) != tt.wantErr {
				t.Errorf("processAggReply() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if gotTotal != tt.wantTotal {
				t.Errorf("processAggReply() gotTotal = %v, want %v", gotTotal, tt.wantTotal)
			}
			if !reflect.DeepEqual(gotAggregateReply, tt.wantAggregateReply) {
				t.Errorf("processAggReply() gotAggregateReply = %v, want %v", gotAggregateReply, tt.wantAggregateReply)
			}
		})
	}
}

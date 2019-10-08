package redisearch_test

import (
	"bufio"
	"compress/bzip2"
	"encoding/json"
	"fmt"
	"github.com/RediSearch/redisearch-go/redisearch"
	"github.com/stretchr/testify/assert"
	"log"
	"math/rand"
	"os"
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
	assert.Equal(t, 10, count)
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

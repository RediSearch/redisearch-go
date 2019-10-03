package redisearch_test

import (
	"bufio"
	"compress/bzip2"
	"encoding/json"
	"fmt"
	"github.com/RediSearch/redisearch-go/redisearch"
	"github.com/stretchr/testify/assert"
	"log"
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
	assert.Equal(t, 292, count)
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

func TestAggregateFTSB(t *testing.T) {
	c := createClient("pages-meta-idx1")

	sc := redisearch.NewSchema(redisearch.DefaultOptions).
		AddField(redisearch.NewTextFieldOptions("TITLE", redisearch.TextFieldOptions{Sortable: true})).
		AddField(redisearch.NewTagFieldOptions("NAMESPACE", redisearch.TagFieldOptions{Sortable: true})).
		AddField(redisearch.NewSortableNumericField("ID")).
		AddField(redisearch.NewSortableNumericField("PARENT_REVISION_ID")).
		AddField(redisearch.NewSortableNumericField("CURRENT_REVISION_TIMESTAMP")).
		AddField(redisearch.NewSortableNumericField("CURRENT_REVISION_ID")).
		AddField(redisearch.NewTextFieldOptions("CURRENT_REVISION_EDITOR_USERNAME", redisearch.TextFieldOptions{NoStem: true})).
		AddField(redisearch.NewTextField("CURRENT_REVISION_EDITOR_IP")).
		AddField(redisearch.NewSortableNumericField("CURRENT_REVISION_EDITOR_USERID")).
		AddField(redisearch.NewTextField("CURRENT_REVISION_EDITOR_COMMENT")).
		AddField(redisearch.NewSortableNumericField("CURRENT_REVISION_CONTENT_LENGTH"))
	c.Drop()
	assert.Nil(t, c.CreateIndex(sc))

	docs := make([]redisearch.Document, 0)
	docs = append(docs, redisearch.NewDocument(fmt.Sprintf("pages-meta-idx1-%d", 1), 1).
		Set("NAMESPACE", "1").
		Set("ID", 1).
		Set("CURRENT_REVISION_TIMESTAMP", 1540378169).
		Set("CURRENT_REVISION_EDITOR_USERNAME", "Narky Blert").
		Set("CURRENT_REVISION_CONTENT_LENGTH", 2),
	)
	docs = append(docs, redisearch.NewDocument(fmt.Sprintf("pages-meta-idx1-%d", 2), 1).
		Set("NAMESPACE", "0").
		Set("ID", 2).
		Set("CURRENT_REVISION_TIMESTAMP", 1447349117).
		Set("CURRENT_REVISION_EDITOR_USERNAME", "CZmarlin").
		Set("CURRENT_REVISION_CONTENT_LENGTH", 50),
	)
	docs = append(docs, redisearch.NewDocument(fmt.Sprintf("pages-meta-idx1-%d", 3), 1).
		Set("NAMESPACE", "0").
		Set("ID", 3).
		Set("CURRENT_REVISION_TIMESTAMP", 1427349117).
		Set("CURRENT_REVISION_EDITOR_USERNAME", "CZmarlin").
		Set("CURRENT_REVISION_CONTENT_LENGTH", 50),
	)

	docs = append(docs, redisearch.NewDocument(fmt.Sprintf("pages-meta-idx1-%d", 4), 1).
		Set("NAMESPACE", "0").
		Set("ID", 4).
		Set("CURRENT_REVISION_TIMESTAMP", 1427349110).
		Set("CURRENT_REVISION_EDITOR_USERNAME", "CZmarlin").
		Set("CURRENT_REVISION_CONTENT_LENGTH", 10),
	)

	docs = append(docs, redisearch.NewDocument(fmt.Sprintf("pages-meta-idx1-%d", 5), 1).
		Set("NAMESPACE", "0").
		Set("ID", 5).
		Set("CURRENT_REVISION_TIMESTAMP", 1427349130).
		Set("CURRENT_REVISION_EDITOR_USERNAME", "Jon").
		Set("CURRENT_REVISION_CONTENT_LENGTH", 20),
	)

	docs = append(docs, redisearch.NewDocument(fmt.Sprintf("pages-meta-idx1-%d", 6), 1).
		Set("NAMESPACE", "0").
		Set("ID", 6).
		Set("CURRENT_REVISION_TIMESTAMP", 1427349130).
		Set("CURRENT_REVISION_EDITOR_USERNAME", "Doe").
		Set("CURRENT_REVISION_CONTENT_LENGTH", 49),
	)

	c.Index(docs...)

	//1) One year period, Exact Number of contributions by day, ordered chronologically
	q1 := redisearch.NewAggregateQuery().
		SetMax(365).
		Apply(*redisearch.NewProjection("@CURRENT_REVISION_TIMESTAMP - (@CURRENT_REVISION_TIMESTAMP % 86400)", "day")).
		GroupBy(*redisearch.NewGroupBy().AddFields("@day").
			Reduce(*redisearch.NewReducerAlias(redisearch.GroupByReducerCount, []string{"@ID"}, "num_contributions"))).
		SortBy([]redisearch.SortingKey{*redisearch.NewSortingKeyDir("@day", false)}).
		Apply(*redisearch.NewProjection("timefmt(@day)", "day"))

	resq1, _, err := c.Aggregate(q1)
	assert.Nil(t, err)
	fmt.Printf("%v\n", resq1)

	//2) One month period, Exact Number of distinct editors contributions by hour, ordered chronologically
	q2 := redisearch.NewAggregateQuery().
		SetMax(720).
		Apply(*redisearch.NewProjection("@CURRENT_REVISION_TIMESTAMP - (@CURRENT_REVISION_TIMESTAMP % 3600)", "hour")).
		GroupBy(*redisearch.NewGroupBy().AddFields("@hour").
			Reduce(*redisearch.NewReducerAlias(redisearch.GroupByReducerCount, []string{"@CURRENT_REVISION_EDITOR_USERNAME"}, "num_distinct_editors"))).
		SortBy([]redisearch.SortingKey{*redisearch.NewSortingKeyDir("@hour", false)}).
		Apply(*redisearch.NewProjection("timefmt(@hour)", "hour"))

	resq2, _, err := c.Aggregate(q2)
	assert.Nil(t, err)
	fmt.Printf("%v\n", resq2)

	//3) One month period, Approximate Number of distinct editors contributions by hour, ordered chronologically
	q3 := redisearch.NewAggregateQuery().
		SetMax(720).
		Apply(*redisearch.NewProjection("@CURRENT_REVISION_TIMESTAMP - (@CURRENT_REVISION_TIMESTAMP % 3600)", "hour")).
		GroupBy(*redisearch.NewGroupBy().AddFields("@hour").
			Reduce(*redisearch.NewReducerAlias(redisearch.GroupByReducerCountDistinctish, []string{"@CURRENT_REVISION_EDITOR_USERNAME"}, "num_distinct_editors"))).
		SortBy([]redisearch.SortingKey{*redisearch.NewSortingKeyDir("@hour", false)}).
		Apply(*redisearch.NewProjection("timefmt(@hour)", "hour"))

	resq3, _, err := c.Aggregate(q3)
	assert.Nil(t, err)
	fmt.Printf("%v\n", resq3)

	//4) One day period, Approximate Number of contributions by 5minutes interval by editor username, ordered first chronologically and second alphabetically by Revision editor username
	q4 := redisearch.NewAggregateQuery().
		SetMax(288).
		Apply(*redisearch.NewProjection("@CURRENT_REVISION_TIMESTAMP - (@CURRENT_REVISION_TIMESTAMP % 300)", "fiveMinutes")).
		GroupBy(*redisearch.NewGroupBy().AddFields([]string{"@fiveMinutes", "@CURRENT_REVISION_EDITOR_USERNAME"}).
			Reduce(*redisearch.NewReducerAlias(redisearch.GroupByReducerCountDistinctish, []string{"@ID"}, "num_contributions"))).
		Filter("@CURRENT_REVISION_EDITOR_USERNAME !=\"\"").
		SortBy([]redisearch.SortingKey{*redisearch.NewSortingKeyDir("@fiveMinutes", true), *redisearch.NewSortingKeyDir("@CURRENT_REVISION_EDITOR_USERNAME", false)}).
		Apply(*redisearch.NewProjection("timefmt(@fiveMinutes)", "fiveMinutes"))

	resq4, _, err := c.Aggregate(q4)
	assert.Nil(t, err)
	fmt.Printf("%v\n", resq4)

	//5) Aproximate All time Top 10 Revision editor usernames
	q5 := redisearch.NewAggregateQuery().
		GroupBy(*redisearch.NewGroupBy().AddFields("@CURRENT_REVISION_EDITOR_USERNAME").
			Reduce(*redisearch.NewReducerAlias(redisearch.GroupByReducerCountDistinctish, []string{"@ID"}, "num_contributions"))).
		Filter("@CURRENT_REVISION_EDITOR_USERNAME !=\"\"").
		SortBy([]redisearch.SortingKey{*redisearch.NewSortingKeyDir("@num_contributions", true)}).
		Limit(0, 10)

	resq5, _, err := c.Aggregate(q5)
	assert.Nil(t, err)
	fmt.Printf("%v\n", resq5)

	//6) Aproximate All time Top 10 Revision editor usernames by namespace (TAG field)
	q6 := redisearch.NewAggregateQuery().
		GroupBy(*redisearch.NewGroupBy().AddFields([]string{"@NAMESPACE", "@CURRENT_REVISION_EDITOR_USERNAME"}).
			Reduce(*redisearch.NewReducerAlias(redisearch.GroupByReducerCountDistinctish, []string{"@ID"}, "num_contributions"))).
		Filter("@CURRENT_REVISION_EDITOR_USERNAME !=\"\"").
		SortBy([]redisearch.SortingKey{*redisearch.NewSortingKeyDir("@NAMESPACE", true), *redisearch.NewSortingKeyDir("@num_contributions", true)}).
		Limit(0, 10)

	_, resq6, err := c.Aggregate(q6)
	assert.Nil(t, err)
	fmt.Printf("%v\n", resq6)

	//7) Top 10 editor username by average revision content
	q7 := redisearch.NewAggregateQuery().
		GroupBy(*redisearch.NewGroupBy().AddFields([]string{"@NAMESPACE", "@CURRENT_REVISION_EDITOR_USERNAME"}).
			Reduce(*redisearch.NewReducerAlias(redisearch.GroupByReducerAvg, []string{"@CURRENT_REVISION_CONTENT_LENGTH"}, "avg_rcl"))).
		SortBy([]redisearch.SortingKey{*redisearch.NewSortingKeyDir("@avg_rcl", false)}).
		Limit(0, 10)

	resq7, _, err := c.Aggregate(q7)
	assert.Nil(t, err)
	fmt.Printf("%v\n", resq7)

	//8) Aproximate average number of contributions by year each editor makes
	q8 := redisearch.NewAggregateQuery().
		Apply(*redisearch.NewProjection("year(@CURRENT_REVISION_TIMESTAMP)", "year")).
		GroupBy(*redisearch.NewGroupBy().AddFields("@year").
			Reduce(*redisearch.NewReducerAlias(redisearch.GroupByReducerCount, []string{"@ID"}, "num_contributions")).
			Reduce(*redisearch.NewReducerAlias(redisearch.GroupByReducerCountDistinctish, []string{"@CURRENT_REVISION_EDITOR_USERNAME"}, "num_distinct_editors"))).
		Apply(*redisearch.NewProjection("@num_contributions / @num_distinct_editors", "avg_num_contributions_by_editor")).
		SortBy([]redisearch.SortingKey{*redisearch.NewSortingKeyDir("@year", true)})

	resq8, _, err := c.Aggregate(q8)
	assert.Nil(t, err)
	fmt.Printf("%v\n", resq8)
}

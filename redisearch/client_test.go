package redisearch_test

import (
	"fmt"
	"github.com/RediSearch/redisearch-go/redisearch"
	"log"
	"os"
	"testing"
)

func createBenchClient(indexName string) *redisearch.Client {
	value, exists := os.LookupEnv("REDISEARCH_TEST_HOST")
	host := "localhost:6379"
	if exists && value != "" {
		host = value
	}
	return redisearch.NewClient(host, indexName)
}

func init() {
	/* load test data */
	value, exists := os.LookupEnv("REDISEARCH_RDB_LOADED")
	requiresDatagen := true
	if exists && value != "" {
		requiresDatagen = false
	}
	if requiresDatagen {
		c := createBenchClient("bench.ft.aggregate")

		sc := redisearch.NewSchema(redisearch.DefaultOptions).
			AddField(redisearch.NewTextField("foo"))
		c.Drop()
		if err := c.CreateIndex(sc); err != nil {
			log.Fatal(err)
		}
		ndocs := 10000
		docs := make([]redisearch.Document, ndocs)
		for i := 0; i < ndocs; i++ {
			docs[i] = redisearch.NewDocument(fmt.Sprintf("doc%d", i), 1).Set("foo", "hello world")
		}

		if err := c.IndexOptions(redisearch.DefaultIndexingOptions, docs...); err != nil {
			log.Fatal(err)
		}
	}

}

func benchmarkAggregate(c *redisearch.Client, q *redisearch.AggregateQuery, b *testing.B) {
	for n := 0; n < b.N; n++ {
		c.Aggregate(q)
	}
}

func benchmarkAggregateCursor(c *redisearch.Client, q *redisearch.AggregateQuery, b *testing.B) {
	for n := 0; n < b.N; n++ {
		c.Aggregate(q)
		for q.CursorHasResults() {
			c.Aggregate(q)
		}
	}
}

func BenchmarkAgg_1(b *testing.B) {
	c := createBenchClient("bench.ft.aggregate")
	q := redisearch.NewAggregateQuery().
		SetQuery(redisearch.NewQuery("*"))
	b.ResetTimer()
	benchmarkAggregate(c, q, b)
}

func BenchmarkAggCursor_1(b *testing.B) {
	c := createBenchClient("bench.ft.aggregate")
	q := redisearch.NewAggregateQuery().
		SetQuery(redisearch.NewQuery("*")).
		SetCursor(redisearch.NewCursor())
	b.ResetTimer()
	benchmarkAggregateCursor(c, q, b)
}

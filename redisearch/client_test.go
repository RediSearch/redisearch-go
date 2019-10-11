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
	c := createBenchClient("bench.ft.aggregate.cursor")

	sc := redisearch.NewSchema(redisearch.DefaultOptions).
		AddField(redisearch.NewTextField("foo"))
	c.Drop()
	if err := c.CreateIndex(sc); err != nil {
		log.Fatal(err)
	}

	docs := make([]redisearch.Document, 100000)
	for i := 0; i < 100000; i++ {
		docs[i] = redisearch.NewDocument(fmt.Sprintf("doc%d", i), 1).Set("foo", "hello world")
	}

	if err := c.IndexOptions(redisearch.DefaultIndexingOptions, docs...); err != nil {
		log.Fatal(err)
	}
}

func benchmarkAggregate(c *redisearch.Client, q* redisearch.AggregateQuery, b *testing.B) {
	for n := 0; n < b.N; n++ {
		c.Aggregate(q)
		for q.Cursor.Id != 0 {
			c.Aggregate(q)
		}
	}
}

func BenchmarkAggCursor_1(b *testing.B) {
	c := createBenchClient("bench.ft.aggregate.cursor")
	q:= redisearch.NewAggregateQuery().
		SetQuery(redisearch.NewQuery("*")).
		SetCursor(redisearch.NewCursor())
	c.Aggregate(q)
	b.ResetTimer()
	benchmarkAggregate(c, q, b)
}

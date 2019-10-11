package redisearch_test

import (
	"fmt"
	"github.com/RediSearch/redisearch-go/redisearch"
	"log"
	"testing"
)

func init() {
	/* load test data */
	c := createClient("bench.ft.aggregate.cursor")

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
	c := createClient("bench.ft.aggregate.cursor")
	q:= redisearch.NewAggregateQuery().
		SetQuery(redisearch.NewQuery("*")).
		SetCursor(redisearch.NewCursor())
	c.Aggregate(q)
	b.ResetTimer()
	benchmarkAggregate(c, q, b)
}

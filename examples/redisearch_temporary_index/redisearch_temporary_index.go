package main

import (
	"fmt"
	"log"
	"time"

	"github.com/RediSearch/redisearch-go/redisearch"
)

/**
 * This demo demonstrates the usage of CreateIndex(), to create a lightweight temporary index that will expire after the specified period of inactivity.
 * The internal idle timer is reset whenever the index is searched or added to.
 * Because such indexes are lightweight, you can create thousands of such indexes without negative performance implications.
 */
func main() {
	// Create a client. By default a client is schemaless
	// unless a schema is provided when creating the index
	c := redisearch.NewClient("localhost:6379", "myTemporaryIndex")

	// Create a schema with a temporary index with temporary period of 10s
	sc := redisearch.NewSchema(*redisearch.NewOptions().SetTemporaryPeriod(10)).
		AddField(redisearch.NewTextField("body")).
		AddField(redisearch.NewTextFieldOptions("title", redisearch.TextFieldOptions{Weight: 5.0, Sortable: true})).
		AddField(redisearch.NewNumericField("date"))

	// Drop an existing index. If the index does not exist an error is returned
	c.Drop()

	// Create the index with the given schema
	if err := c.CreateIndex(sc); err != nil {
		log.Fatal(err)
	}

	// Create a document with an id and given score
	doc := redisearch.NewDocument("doc1", 1.0)
	doc.Set("title", "Hello world").
		Set("body", "foo bar").
		Set("date", time.Now().Unix())

	// Index the document. The API accepts multiple documents at a time
	if err := c.IndexOptions(redisearch.DefaultIndexingOptions, doc); err != nil {
		log.Fatal(err)
	}

	docs, total, err := c.Search(redisearch.NewQuery("hello world").
		Limit(0, 2).
		SetReturnFields("title"))

	// Verify that the we're able to search on the temporary created index
	fmt.Println(docs[0].Id, docs[0].Properties["title"], total, err)
	// Output: doc1 Hello world 1 <nil>

	time.Sleep(15 * time.Second)
	// Searching with limit and sorting
	_, err = c.Info()
	fmt.Println(err)
	// Output: Unknown Index name
}

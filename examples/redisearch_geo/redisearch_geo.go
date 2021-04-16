package main

import (
	"fmt"
	"github.com/RediSearch/redisearch-go/redisearch"
	"log"
)

/**
 * This demo should be updated in RediSearch.io if changed
 * Update at: https://github.com/RediSearch/RediSearch/blob/master/docs/go_client.md
 */
func main() {
	// Create a client. By default a client is schemaless
	// unless a schema is provided when creating the index
	c := redisearch.NewClient("localhost:6379", "myIndex")

	// Create a schema
	sc := redisearch.NewSchema(redisearch.DefaultOptions).
		AddField(redisearch.NewTextField("body")).
		AddField(redisearch.NewTextFieldOptions("title", redisearch.TextFieldOptions{Weight: 5.0, Sortable: true})).
		AddField(redisearch.NewGeoField("location"))

	// Drop an existing index. If the index does not exist an error is returned
	c.Drop()

	// Create the index with the given schema
	if err := c.CreateIndex(sc); err != nil {
		log.Fatal(err)
	}

	// Create a document with an id and given score
	// Note While Specifying location you should specify in following order -> latitude and longitude
	doc := redisearch.NewDocument("doc1", 1.0)
	doc.Set("title", "Hello world").
		Set("body", "foo bar").
		Set("location", "23.147782245408465,80.0566448028139")

	// Index the document. The API accepts multiple documents at a time
	if err := c.IndexOptions(redisearch.DefaultIndexingOptions, doc); err != nil {
		log.Fatal(err)
	}

	// Searching with limit and sorting
	docs, total, err := c.Search(redisearch.NewQuery("*").AddFilter(
		redisearch.Filter{
			Field: "location",
			Options: redisearch.GeoFilterOptions{
				Lon:    23.147690778131295,
				Lat:    80.0564903169592,
				Radius: 100,
				Unit:   redisearch.METERS,
			},
		},
	).Limit(0, 2))

	fmt.Println(docs, total, err)
	// Output: doc1 Hello world 1 <nil>
}

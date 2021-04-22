package main

import (
	"fmt"
	"github.com/RediSearch/redisearch-go/redisearch"
	"log"
)

/**
 * This example maps to https://redis.io/commands/geoadd#examples of Georadius search
 * This demo should be updated in RediSearch.io if changed
 * Update at: https://github.com/RediSearch/RediSearch/blob/master/docs/go_client.md
 */
func main() {
	// Create a client. By default a client is schemaless
	// unless a schema is provided when creating the index
	c := redisearch.NewClient("localhost:6379", "cityIndex")

	// Create a schema
	sc := redisearch.NewSchema(redisearch.DefaultOptions).
		AddField(redisearch.NewTextField("city")).
		AddField(redisearch.NewGeoField("location"))

	// Drop an existing index. If the index does not exist an error is returned
	c.Drop()

	// Create the index with the given schema
	if err := c.CreateIndex(sc); err != nil {
		log.Fatal(err)
	}

	// Create the city docs
	// Note While Specifying location you should specify in following order -> longitude,latitude
	// Same look and feel as GEOADD https://redis.io/commands/geoadd
	// This example maps to https://redis.io/commands/geoadd#examples
	docPalermo := redisearch.NewDocument("doc:Palermo", 1.0)
	docPalermo.Set("name", "Palermo").
		Set("location", "13.361389,38.115556")

	docCatania := redisearch.NewDocument("doc:Catania", 1.0)
	docCatania.Set("name", "Catania").
		Set("location", "15.087269,37.502669")

	// Index the documents. The API accepts multiple documents at a time
	if err := c.IndexOptions(redisearch.DefaultIndexingOptions, docPalermo, docCatania); err != nil {
		log.Fatal(err)
	}

	// Searching for 100KM radius should only output Catania
	docs, _, _ := c.Search(redisearch.NewQuery("*").AddFilter(
		redisearch.Filter{
			Field: "location",
			Options: redisearch.GeoFilterOptions{
				Lon:    15,
				Lat:    37,
				Radius: 100,
				Unit:   redisearch.KILOMETERS,
			},
		},
	).Limit(0, 2))

	fmt.Println("100KM Radius search from longitude 15 latitude 37")
	fmt.Println(docs[0])
	// Output: 100KM Radius search from longitude 15 latitude 37
	// Output: {doc:Catania 1 [] map[location:15.087269,37.502669 name:Catania]}

	// Searching for 200KM radius should output Catania and Palermo
	docs, _, _ = c.Search(redisearch.NewQuery("*").AddFilter(
		redisearch.Filter{
			Field: "location",
			Options: redisearch.GeoFilterOptions{
				Lon:    15,
				Lat:    37,
				Radius: 200,
				Unit:   redisearch.KILOMETERS,
			},
		},
	).Limit(0, 2).SetSortBy("location", true))
	fmt.Println("200KM Radius search from longitude 15 latitude 37")
	fmt.Println(docs[0])
	fmt.Println(docs[1])
	// Output: 100KM Radius search from longitude 15 latitude 37
	// Output: {doc:Palermo 1 [] map[location:13.361389,38.115556 name:Palermo]}
	// Output: {doc:Catania 1 [] map[location:15.087269,37.502669 name:Catania]}
}

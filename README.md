[![license](https://img.shields.io/github/license/RediSearch/redisearch-go.svg)](https://github.com/RediSearch/redisearch-go)
[![CircleCI](https://circleci.com/gh/RediSearch/redisearch-go/tree/master.svg?style=svg)](https://circleci.com/gh/RediSearch/redisearch-go/tree/master)
[![GitHub issues](https://img.shields.io/github/release/RediSearch/redisearch-go.svg)](https://github.com/RediSearch/redisearch-go/releases/latest)
[![Codecov](https://codecov.io/gh/RediSearch/redisearch-go/branch/master/graph/badge.svg)](https://codecov.io/gh/RediSearch/redisearch-go)
[![Go Report Card](https://goreportcard.com/badge/github.com/RediSearch/redisearch-go)](https://goreportcard.com/report/github.com/RediSearch/redisearch-go)
[![GoDoc](https://godoc.org/github.com/RediSearch/redisearch-go?status.svg)](https://godoc.org/github.com/RediSearch/redisearch-go)


# RediSearch Go Client

Go client for [RediSearch](http://redisearch.io), based on redigo.

# Installing 

```sh
go get github.com/RediSearch/redisearch-go/redisearch
```

# Usage Example

```go

import (
	"fmt"
	"log"
	"time"

	"github.com/RediSearch/redisearch-go/redisearch"
)

func ExampleClient() {

	// Create a client. By default a client is schemaless
	// unless a schema is provided when creating the index
	c := redisearch.NewClient("localhost:6379", "myIndex")

	// Create a schema
	sc := redisearch.NewSchema(redisearch.DefaultOptions).
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
	if err := c.Index([]redisearch.Document{doc}...); err != nil {
		log.Fatal(err)
	}

	// Searching with limit and sorting
	docs, total, err := c.Search(redisearch.NewQuery("hello world").
		Limit(0, 2).
		SetReturnFields("title"))

	fmt.Println(docs[0].Id, docs[0].Properties["title"], total, err)
	// Output: doc1 Hello world 1 <nil>
}
```

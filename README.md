[![license](https://img.shields.io/github/license/RediSearch/redisearch-go.svg)](https://github.com/RediSearch/redisearch-go)
[![CircleCI](https://circleci.com/gh/RediSearch/redisearch-go/tree/master.svg?style=svg)](https://circleci.com/gh/RediSearch/redisearch-go/tree/master)
[![GitHub issues](https://img.shields.io/github/release/RediSearch/redisearch-go.svg)](https://github.com/RediSearch/redisearch-go/releases/latest)
[![Codecov](https://codecov.io/gh/RediSearch/redisearch-go/branch/master/graph/badge.svg)](https://codecov.io/gh/RediSearch/redisearch-go)
[![Go Report Card](https://goreportcard.com/badge/github.com/RediSearch/redisearch-go)](https://goreportcard.com/report/github.com/RediSearch/redisearch-go)
[![GoDoc](https://godoc.org/github.com/RediSearch/redisearch-go?status.svg)](https://godoc.org/github.com/RediSearch/redisearch-go)


# RediSearch Go Client
[![Mailing List](https://img.shields.io/badge/Mailing%20List-RediSearch-blue)](https://groups.google.com/forum/#!forum/redisearch)
[![Gitter](https://badges.gitter.im/RedisLabs/RediSearch.svg)](https://gitter.im/RedisLabs/RediSearch?utm_source=badge&utm_medium=badge&utm_campaign=pr-badge)

Go client for [RediSearch](http://redisearch.io), based on redigo.

# Installing 

```sh
go get github.com/RediSearch/redisearch-go/redisearch
```

# Usage Example

```go
package main 
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


## Supported RediSearch Commands

| Command | Recommended API and godoc  |
| :---          |  ----: |
| [FT.CREATE](https://oss.redislabs.com/redisearch/Commands.html#ftcreate) |   [CreateIndex](https://godoc.org/github.com/RediSearch/redisearch-go/redisearch#Client.CreateIndex)          |
| [FT.ADD](https://oss.redislabs.com/redisearch/Commands.html#ftadd) |   [IndexOptions](https://godoc.org/github.com/RediSearch/redisearch-go/redisearch#Client.IndexOptions)          |
| [FT.ADDHASH](https://oss.redislabs.com/redisearch/Commands.html#ftaddhash) | N/A |
| [FT.ALTER](https://oss.redislabs.com/redisearch/Commands.html#ftalter) |    N/A |
| [FT.ALIASADD](https://oss.redislabs.com/redisearch/Commands.html#ftaliasadd) |    N/A         |
| [FT.ALIASUPDATE](https://oss.redislabs.com/redisearch/Commands.html#ftaliasupdate) |     N/A         |
| [FT.ALIASDEL](https://oss.redislabs.com/redisearch/Commands.html#ftaliasdel) |     N/A         |
| [FT.INFO](https://oss.redislabs.com/redisearch/Commands.html#ftinfo) |   [Info](https://godoc.org/github.com/RediSearch/redisearch-go/redisearch#Client.Info)          |
| [FT.SEARCH](https://oss.redislabs.com/redisearch/Commands.html#ftsearch) |  [Search](https://godoc.org/github.com/RediSearch/redisearch-go/redisearch#Client.Search)          |
| [FT.AGGREGATE](https://oss.redislabs.com/redisearch/Commands.html#ftaggregate) |   [Aggregate](https://godoc.org/github.com/RediSearch/redisearch-go/redisearch#Client.Aggregate)          |
| [FT.CURSOR](https://oss.redislabs.com/redisearch/Aggregations.html#cursor_api) |   [Aggregate](https://godoc.org/github.com/RediSearch/redisearch-go/redisearch#Client.Aggregate) + (*WithCursor option set to True)         |
| [FT.EXPLAIN](https://oss.redislabs.com/redisearch/Commands.html#ftexplain) |   [Explain](https://godoc.org/github.com/RediSearch/redisearch-go/redisearch#Client.Explain)        |
| [FT.DEL](https://oss.redislabs.com/redisearch/Commands.html#ftdel) |   [Delete](https://godoc.org/github.com/RediSearch/redisearch-go/redisearch#Client.Delete)        |
| [FT.GET](https://oss.redislabs.com/redisearch/Commands.html#ftget) |    N/A |
| [FT.MGET](https://oss.redislabs.com/redisearch/Commands.html#ftmget) |    N/A |
| [FT.DROP](https://oss.redislabs.com/redisearch/Commands.html#ftdrop) |   [Drop](https://godoc.org/github.com/RediSearch/redisearch-go/redisearch#Client.Drop)        |
| [FT.TAGVALS](https://oss.redislabs.com/redisearch/Commands.html#fttagvals) |    N/A |
| [FT.SUGADD](https://oss.redislabs.com/redisearch/Commands.html#ftsugadd) |    [AddTerms](https://godoc.org/github.com/RediSearch/redisearch-go/redisearch#Autocompleter.AddTerms) |
| [FT.SUGGET](https://oss.redislabs.com/redisearch/Commands.html#ftsugget) |    [SuggestOpts](https://godoc.org/github.com/RediSearch/redisearch-go/redisearch#Autocompleter.SuggestOpts)  |
| [FT.SUGDEL](https://oss.redislabs.com/redisearch/Commands.html#ftsugdel) |    [DeleteTerms](https://godoc.org/github.com/RediSearch/redisearch-go/redisearch#Autocompleter.DeleteTerms)  |
| [FT.SUGLEN](https://oss.redislabs.com/redisearch/Commands.html#ftsuglen) |    [Autocompleter.Length](https://godoc.org/github.com/RediSearch/redisearch-go/redisearch#Autocompleter.Length)  |
| [FT.SYNADD](https://oss.redislabs.com/redisearch/Commands.html#ftsynadd) |    N/A |
| [FT.SYNUPDATE](https://oss.redislabs.com/redisearch/Commands.html#ftsynupdate) |    N/A |
| [FT.SYNDUMP](https://oss.redislabs.com/redisearch/Commands.html#ftsyndump) |    N/A |
| [FT.SPELLCHECK](https://oss.redislabs.com/redisearch/Commands.html#ftspellcheck) |  [SpellCheck](https://godoc.org/github.com/RediSearch/redisearch-go/redisearch#Client.SpellCheck)        |
| [FT.DICTADD](https://oss.redislabs.com/redisearch/Commands.html#ftdictadd) |    N/A |
| [FT.DICTDEL](https://oss.redislabs.com/redisearch/Commands.html#ftdictdel) |    N/A |
| [FT.DICTDUMP](https://oss.redislabs.com/redisearch/Commands.html#ftdictdump) |    N/A |
| [FT.CONFIG](https://oss.redislabs.com/redisearch/Commands.html#ftconfig) |    N/A |


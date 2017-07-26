# RediSearch Go Client

Go client for [RediSearch](http://redisearch.io), based on redigo.

# Installing 

```sh
go get github.com/RedisLabs/redisearch-go/redisearch
```

# Usage Example

```go

import (
	"fmt"
	"log"
	"time"

	"github.com/RedisLabs/redisearch-go/redisearch"
)
func TestClient(t *testing.T) {

	c := redisearch.NewClient("localhost:6379", "testung")

	sc := redisearch.NewSchema(redisearch.DefaultOptions).
		AddField(redisearch.NewTextField("foo"))
	c.Drop()
	if err := c.CreateIndex(sc); err != nil {
		t.Fatal(err)
	}

	docs := make([]redisearch.Document, 100)
	for i := 0; i < 100; i++ {
		docs[i] = redisearch.NewDocument(fmt.Sprintf("doc%d", i), float32(i)/float32(100)).Set("foo", "hello world")
	}

	if err := c.Index(docs, redisearch.DefaultIndexingOptions); err != nil {
		t.Fatal(err)
	}

	docs, total, err := c.Search(redisearch.NewQuery("hello world"))
	fmt.Println(docs, total, err)
}

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
	if err := c.Index([]redisearch.Document{doc},
		redisearch.DefaultIndexingOptions); err != nil {
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
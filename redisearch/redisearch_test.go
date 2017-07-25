package redisearch

import (
	"fmt"
	"testing"
)

func TestClient(t *testing.T) {

	c := NewClient("localhost:6379", "testung")

	sc := NewSchema(DefaultOptions).AddField(NewTextField("foo"))
	c.Drop()
	if err := c.CreateIndex(sc); err != nil {
		t.Fatal(err)
	}

	docs := make([]Document, 100)
	for i := 0; i < 100; i++ {
		docs[i] = NewDocument(fmt.Sprintf("doc%d", i), float32(i)/float32(100)).Set("foo", "hello world")
	}

	if err := c.Index(docs, DefaultIndexingOptions); err != nil {
		t.Fatal(err)
	}

	docs, total, err := c.Search(*NewQuery("hello world"))
	fmt.Println(docs, total, err)
}

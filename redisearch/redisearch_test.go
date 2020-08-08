package redisearch

import (
	"fmt"
	"log"
	"os"
	"testing"
	"time"

	"github.com/gomodule/redigo/redis"

	"github.com/stretchr/testify/assert"
)

func getTestConnectionDetails() (string, string) {
	value, exists := os.LookupEnv("REDISEARCH_TEST_HOST")
	host := "localhost:6379"
	password := ""
	valuePassword, existsPassword := os.LookupEnv("REDISEARCH_TEST_PASSWORD")
	if exists && value != "" {
		host = value
	}
	if existsPassword && valuePassword != "" {
		password = valuePassword
	}
	return host, password
}

func createClient(indexName string) *Client {
	host, password := getTestConnectionDetails()
	if password != "" {
		pool := &redis.Pool{Dial: func() (redis.Conn, error) {
			return redis.Dial("tcp", host, redis.DialPassword(password))
		}, MaxIdle: maxConns}
		pool.TestOnBorrow = func(c redis.Conn, t time.Time) (err error) {
			if time.Since(t) > time.Second {
				_, err = c.Do("PING")
			}
			return err
		}
		return NewClientFromPool(pool, indexName)
	} else {
		return NewClient(host, indexName)
	}
}

func createAutocompleter(dictName string) *Autocompleter {
	host, password := getTestConnectionDetails()
	if password != "" {
		pool := &redis.Pool{Dial: func() (redis.Conn, error) {
			return redis.Dial("tcp", host, redis.DialPassword(password))
		}, MaxIdle: maxConns}
		pool.TestOnBorrow = func(c redis.Conn, t time.Time) (err error) {
			if time.Since(t) > time.Second {
				_, err = c.Do("PING")
			}
			return err
		}
		return NewAutocompleterFromPool(pool, dictName)
	} else {
		return NewAutocompleter(host, dictName)
	}
}

func TestClient(t *testing.T) {

	c := createClient("testung")

	sc := NewSchema(DefaultOptions).
		AddField(NewTextField("foo"))
	c.Drop()

	if err := c.CreateIndex(sc); err != nil {
		t.Fatal(err)
	}

	docs := make([]Document, 100)
	for i := 0; i < 100; i++ {
		docs[i] = NewDocument(fmt.Sprintf("TestClient-doc%d", i), float32(i)/float32(100)).Set("foo", "hello world")
	}

	if err := c.IndexOptions(DefaultIndexingOptions, docs...); err != nil {
		t.Fatal(err)
	}

	// Test it again
	if err := c.IndexOptions(DefaultIndexingOptions, docs...); err == nil {
		t.Fatal("Expected error for duplicate document")
	} else {
		if merr, ok := err.(MultiError); !ok {
			t.Fatal("error not a multi error")
		} else {
			assert.Equal(t, 100, len(merr))
			assert.NotEmpty(t, merr)
			//fmt.Println("Got errors: ", merr)
		}
	}

	docs, total, err := c.Search(NewQuery("hello world"))

	assert.Nil(t, err)
	assert.Equal(t, 100, total)
	assert.Equal(t, 10, len(docs))

}

func TestInfo(t *testing.T) {
	c := createClient("testung")

	sc := NewSchema(DefaultOptions).
		AddField(NewTextField("foo")).
		AddField(NewSortableNumericField("bar"))
	c.Drop()

	assert.Nil(t, c.CreateIndex(sc))

	_, err := c.Info()
	assert.Nil(t, err)
}

func TestNumeric(t *testing.T) {
	c := createClient("testung")

	sc := NewSchema(DefaultOptions).
		AddField(NewTextField("foo")).
		AddField(NewSortableNumericField("bar"))
	c.Drop()

	assert.Nil(t, c.CreateIndex(sc))

	docs := make([]Document, 100)
	for i := 0; i < 100; i++ {
		docs[i] = NewDocument(fmt.Sprintf("TestNumeric-doc%d", i), 1).Set("foo", "hello world").Set("bar", i)
	}

	assert.Nil(t, c.Index(docs...))

	docs, total, err := c.Search(NewQuery("hello world @bar:[50 100]").SetFlags(QueryNoContent | QueryWithScores))
	assert.Nil(t, err)
	assert.Equal(t, 10, len(docs))
	assert.Equal(t, 50, total)

	docs, total, err = c.Search(NewQuery("hello world @bar:[40 90]").SetSortBy("bar", false))
	assert.Nil(t, err)
	assert.Equal(t, 10, len(docs))
	assert.Equal(t, 51, total)
	assert.Equal(t, "TestNumeric-doc90", docs[0].Id)
	assert.Equal(t, "TestNumeric-doc89", docs[1].Id)
	assert.Equal(t, "TestNumeric-doc81", docs[9].Id)

	docs, total, err = c.Search(NewQuery("hello world @bar:[40 90]").
		SetSortBy("bar", true).
		SetReturnFields("foo"))
	assert.Nil(t, err)
	assert.Equal(t, 10, len(docs))
	assert.Equal(t, 51, total)
	assert.Equal(t, "TestNumeric-doc40", docs[0].Id)
	assert.Equal(t, "hello world", docs[0].Properties["foo"])
	assert.Nil(t, docs[0].Properties["bar"])
	assert.Equal(t, "TestNumeric-doc41", docs[1].Id)
	assert.Equal(t, "TestNumeric-doc49", docs[9].Id)

	// Try "Explain"
	explain, err := c.Explain(NewQuery("hello world @bar:[40 90]"))
	assert.Nil(t, err)
	assert.NotNil(t, explain)
}

func TestNoIndex(t *testing.T) {
	c := createClient("testung")
	c.Drop()

	sc := NewSchema(DefaultOptions).
		AddField(NewTextFieldOptions("f1", TextFieldOptions{Sortable: true, NoIndex: true, Weight: 1.0})).
		AddField(NewTextField("f2"))

	err := c.CreateIndex(sc)
	assert.Nil(t, err)

	props := make(map[string]interface{})
	props["f1"] = "MarkZZ"
	props["f2"] = "MarkZZ"

	err = c.Index(Document{Id: "TestNoIndex-doc1", Properties: props})
	assert.Nil(t, err)

	props["f1"] = "MarkAA"
	props["f2"] = "MarkAA"
	err = c.Index(Document{Id: "TestNoIndex-doc2", Properties: props})
	assert.Nil(t, err)

	_, total, err := c.Search(NewQuery("@f1:Mark*"))
	assert.Nil(t, err)
	assert.Equal(t, 0, total)

	_, total, err = c.Search(NewQuery("@f2:Mark*"))
	assert.Equal(t, 2, total)

	docs, total, err := c.Search(NewQuery("@f2:Mark*").SetSortBy("f1", false))
	assert.Equal(t, 2, total)
	assert.Equal(t, "TestNoIndex-doc1", docs[0].Id)

	docs, total, err = c.Search(NewQuery("@f2:Mark*").SetSortBy("f1", true))
	assert.Equal(t, 2, total)
	assert.Equal(t, "TestNoIndex-doc2", docs[0].Id)
}

func TestHighlight(t *testing.T) {
	c := createClient("testung")
	c.Drop()

	sc := NewSchema(DefaultOptions).
		AddField(NewTextField("foo")).
		AddField(NewTextField("bar"))

	assert.Nil(t, c.CreateIndex(sc))

	docs := make([]Document, 100)
	for i := 0; i < 100; i++ {
		docs[i] = NewDocument(fmt.Sprintf("doc%d", i), 1).Set("foo", "hello world").Set("bar", "hello world foo bar baz")
	}
	c.Index(docs...)

	q := NewQuery("hello").Highlight([]string{"foo"}, "[", "]")
	docs, _, err := c.Search(q)
	assert.Nil(t, err)

	assert.Equal(t, 10, len(docs))
	for _, d := range docs {
		assert.Equal(t, "[hello] world", d.Properties["foo"])
		assert.Equal(t, "hello world foo bar baz", d.Properties["bar"])
	}

	q = NewQuery("hello world baz").Highlight([]string{"foo", "bar"}, "{", "}")
	docs, _, err = c.Search(q)
	assert.Nil(t, err)

	assert.Equal(t, 10, len(docs))
	for _, d := range docs {
		assert.Equal(t, "{hello} {world}", d.Properties["foo"])
		assert.Equal(t, "{hello} {world} foo bar {baz}", d.Properties["bar"])
	}

	// test RETURN contradicting HIGHLIGHT
	q = NewQuery("hello").Highlight([]string{"foo"}, "[", "]").SetReturnFields("bar")
	docs, _, err = c.Search(q)
	assert.Nil(t, err)

	assert.Equal(t, 10, len(docs))
	for _, d := range docs {
		assert.Equal(t, nil, d.Properties["foo"])
		assert.Equal(t, "hello world foo bar baz", d.Properties["bar"])
	}

	c.Drop()
}

func TestSummarize(t *testing.T) {
	c := createClient("testung")

	sc := NewSchema(DefaultOptions).
		AddField(NewTextField("foo")).
		AddField(NewTextField("bar"))
	c.Drop()

	assert.Nil(t, c.CreateIndex(sc))

	docs := make([]Document, 10)
	for i := 0; i < 10; i++ {
		docs[i] = NewDocument(fmt.Sprintf("TestSummarize-doc%d", i), 1).
			Set("foo", "There are two sub-commands commands used for highlighting. One is HIGHLIGHT which surrounds matching text with an open and/or close tag; and the other is SUMMARIZE which splits a field into contextual fragments surrounding the found terms. It is possible to summarize a field, highlight a field, or perform both actions in the same query.").Set("bar", "hello world foo bar baz")
	}
	c.Index(docs...)

	q := NewQuery("commands fragments fields").Summarize("foo")
	docs, _, err := c.Search(q)
	assert.Nil(t, err)

	assert.Equal(t, 10, len(docs))
	for _, d := range docs {
		assert.Equal(t, "are two sub-commands commands used for highlighting. One is HIGHLIGHT which surrounds... other is SUMMARIZE which splits a field into contextual fragments surrounding the found terms. It is possible to summarize a field, highlight a field, or perform both actions in the... ", d.Properties["foo"])
		assert.Equal(t, "hello world foo bar baz", d.Properties["bar"])
	}

	q = NewQuery("commands fragments fields").
		Highlight([]string{"foo"}, "[", "]").
		SummarizeOptions(SummaryOptions{
			Fields:       []string{"foo"},
			Separator:    "\r\n",
			FragmentLen:  10,
			NumFragments: 5},
		)
	docs, _, err = c.Search(q)
	assert.Nil(t, err)

	assert.Equal(t, 10, len(docs))
	for _, d := range docs {
		assert.Equal(t, "are two sub-[commands] [commands] used for highlighting. One is\r\na [field] into contextual [fragments] surrounding the found terms. It is possible to summarize a [field], highlight a [field], or\r\n", d.Properties["foo"])
		assert.Equal(t, "hello world foo bar baz", d.Properties["bar"])
	}
}

func TestTags(t *testing.T) {
	c := createClient("TestTagsIdx")

	// Create a schema
	sc := NewSchema(DefaultOptions).
		AddField(NewTextField("title")).
		AddField(NewTagFieldOptions("tags", TagFieldOptions{Separator: ';'})).
		AddField(NewTagField("tags2"))

	c.Drop()

	// Create the index with the given schema
	if err := c.CreateIndex(sc); err != nil {
		log.Fatal(err)
	}

	// Create a document with an id and given score
	doc := NewDocument("TestTags-doc1", 1.0)
	doc.Set("title", "Hello world").
		Set("tags", "foo bar;bar,baz;  hello world").
		Set("tags2", "foo bar;bar,baz;  hello world")

	// Index the document. The API accepts multiple documents at a time
	if err := c.IndexOptions(DefaultIndexingOptions, doc); err != nil {
		log.Fatal(err)
	}

	assertNumResults := func(q string, n int) {
		// Searching with limit and sorting
		_, total, err := c.Search(NewQuery(q))
		assert.Nil(t, err)

		assert.Equal(t, n, total)
	}

	assertNumResults("foo bar", 0)
	assertNumResults("@tags:{foo bar}", 1)
	assertNumResults("@tags:{foo\\ bar}", 1)
	assertNumResults("@tags:{bar}", 0)

	assertNumResults("@tags2:{foo\\ bar\\;bar}", 1)
	assertNumResults("@tags:{bar\\,baz}", 1)
	assertNumResults("@tags:{hello world}", 1)
	assertNumResults("@tags:{hello world} @tags2:{foo\\ bar\\;bar}", 1)
	assertNumResults("hello world", 1)

}

func TestDelete(t *testing.T) {
	c := createClient("TestDelete-testung")

	sc := NewSchema(DefaultOptions).
		AddField(NewTextField("foo"))

	err := c.Drop()

	assert.Nil(t, c.CreateIndex(sc))

	var info *IndexInfo

	// validate that the index is empty
	info, err = c.Info()
	assert.Nil(t, err)
	assert.Equal(t, uint64(0), info.DocCount)

	docName := "TestDelete-doc1"
	doc := NewDocument(docName, 1.0)
	doc.Set("foo", "Hello world")

	err = c.IndexOptions(DefaultIndexingOptions, doc)
	assert.Nil(t, err)

	// now we should have 1 document (id = doc1)
	info, err = c.Info()
	assert.Nil(t, err)
	assert.Equal(t, uint64(1), info.DocCount)

	// delete the document reference from the index
	// and the document itself
	err = c.Delete(docName, true)
	assert.Nil(t, err)

	// validate that the index is empty again
	info, err = c.Info()
	assert.Nil(t, err)
	assert.Equal(t, uint64(0), info.DocCount)
	// and that the document was deleted
	conn := c.pool.Get()
	defer conn.Close()
	docExists, err := redis.Bool(conn.Do("EXISTS", docName))
	assert.Nil(t, err)
	assert.False(t, docExists)

	// Re-add the document to the index
	// This time, only remove the reference
	err = c.IndexOptions(DefaultIndexingOptions, doc)
	assert.Nil(t, err)
	err = c.Delete(docName, false)
	assert.Nil(t, err)
	// validate that the index is empty again
	info, err = c.Info()
	assert.Nil(t, err)
	assert.Equal(t, uint64(0), info.DocCount)
	// and that the document remains
	docExists, err = redis.Bool(conn.Do("EXISTS", docName))
	assert.Nil(t, err)
	assert.True(t, docExists)
}

func TestSpellCheck(t *testing.T) {
	c := createClient("testung")
	countries := []string{"Spain", "Israel", "Portugal", "France", "England", "Angola"}
	sc := NewSchema(DefaultOptions).
		AddField(NewTextField("country"))
	c.Drop()

	assert.Nil(t, c.CreateIndex(sc))

	docs := make([]Document, len(countries))

	for i := 0; i < len(countries); i++ {
		docs[i] = NewDocument(fmt.Sprintf("TestSpellCheck-doc%d", i), 1).Set("country", countries[i])
	}

	assert.Nil(t, c.Index(docs...))
	query := NewQuery("Anla Portuga")
	opts := NewSpellCheckOptions(2)
	sugs, total, err := c.SpellCheck(query, opts)
	assert.Nil(t, err)
	assert.Equal(t, 2, len(sugs))
	assert.Equal(t, 2, total)

	// query that return the MisspelledTerm but with an empty list of suggestions
	// 1) 1) "TERM"
	//   2) "an"
	//   3) (empty list or set)
	queryEmpty := NewQuery("An")
	sugs, total, err = c.SpellCheck(queryEmpty, opts)
	assert.Nil(t, err)
	assert.Equal(t, 1, len(sugs))
	assert.Equal(t, 0, total)

	// same query but now with a distance of 4
	opts.SetDistance(4)
	sugs, total, err = c.SpellCheck(queryEmpty, opts)
	assert.Nil(t, err)
	assert.Equal(t, 1, len(sugs))
	assert.Equal(t, 1, total)

}

func TestFilter(t *testing.T) {
	c := createClient("testFilter")
	// Create a schema
	sc := NewSchema(DefaultOptions).
		AddField(NewTextField("body")).
		AddField(NewTextFieldOptions("title", TextFieldOptions{Weight: 5.0, Sortable: true})).
		AddField(NewNumericField("age")).
		AddField(NewGeoFieldOptions("location", GeoFieldOptions{}))

	c.Drop()

	assert.Nil(t, c.CreateIndex(sc))

	// Create a document with an id and given score
	doc := NewDocument("TestFilter-doc1", 1.0)
	doc.Set("title", "Hello world").
		Set("body", "foo bar").
		Set("age", 18).
		Set("location", "13.361389,38.115556")

	assert.Nil(t, c.IndexOptions(DefaultIndexingOptions, doc))
	// Searching with NumericFilter
	docs, total, err := c.Search(NewQuery("hello world").
		AddFilter(Filter{Field: "age", Options: NumericFilterOptions{Min: 1, Max: 20}}).
		SetSortBy("age", true).
		SetReturnFields("body"))
	assert.Nil(t, err)
	assert.Equal(t, 1, total)
	assert.Equal(t, "foo bar", docs[0].Properties["body"])

	// Searching with GeoFilter
	docs, total, err = c.Search(NewQuery("hello world").
		AddFilter(Filter{Field: "location", Options: GeoFilterOptions{Lon: 15, Lat: 37, Radius: 200, Unit: KILOMETERS}}).
		SetSortBy("age", true).
		SetReturnFields("age"))
	assert.Nil(t, err)
	assert.Equal(t, 1, total)
	assert.Equal(t, "18", docs[0].Properties["age"])

	docs, total, err = c.Search(NewQuery("hello world").
		AddFilter(Filter{Field: "location", Options: GeoFilterOptions{Lon: 10, Lat: 13, Radius: 1, Unit: KILOMETERS}}).
		SetSortBy("age", true).
		SetReturnFields("body"))
	assert.Nil(t, err)
	assert.Equal(t, 0, total)
}

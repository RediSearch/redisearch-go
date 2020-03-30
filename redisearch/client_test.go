package redisearch

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"log"
	"os"
	"reflect"
	"testing"
)

func createClient(indexName string) *Client {
	value, exists := os.LookupEnv("REDISEARCH_TEST_HOST")
	host := "localhost:6379"
	if exists && value != "" {
		host = value
	}
	return NewClient(host, indexName)
}

func init() {
	/* load test data */
	value, exists := os.LookupEnv("REDISEARCH_RDB_LOADED")
	requiresDatagen := true
	if exists && value != "" {
		requiresDatagen = false
	}
	if requiresDatagen {
		c := createClient("bench.ft.aggregate")

		sc := NewSchema(DefaultOptions).
			AddField(NewTextField("foo"))
		c.Drop()
		if err := c.CreateIndex(sc); err != nil {
			log.Fatal(err)
		}
		ndocs := 10000
		docs := make([]Document, ndocs)
		for i := 0; i < ndocs; i++ {
			docs[i] = NewDocument(fmt.Sprintf("doc%d", i), 1).Set("foo", "hello world")
		}

		if err := c.IndexOptions(DefaultIndexingOptions, docs...); err != nil {
			log.Fatal(err)
		}
	}

}

func benchmarkAggregate(c *Client, q *AggregateQuery, b *testing.B) {
	for n := 0; n < b.N; n++ {
		c.Aggregate(q)
	}
}

func benchmarkAggregateCursor(c *Client, q *AggregateQuery, b *testing.B) {
	for n := 0; n < b.N; n++ {
		c.Aggregate(q)
		for q.CursorHasResults() {
			c.Aggregate(q)
		}
	}
}

func BenchmarkAgg_1(b *testing.B) {
	c := createClient("bench.ft.aggregate")
	q := NewAggregateQuery().
		SetQuery(NewQuery("*"))
	b.ResetTimer()
	benchmarkAggregate(c, q, b)
}

func BenchmarkAggCursor_1(b *testing.B) {
	c := createClient("bench.ft.aggregate")
	q := NewAggregateQuery().
		SetQuery(NewQuery("*")).
		SetCursor(NewCursor())
	b.ResetTimer()
	benchmarkAggregateCursor(c, q, b)
}

func TestClient_Get(t *testing.T) {

	c := createClient("test-get")
	c.Drop()

	sc := NewSchema(DefaultOptions).
		AddField(NewTextField("foo"))

	if err := c.CreateIndex(sc); err != nil {
		t.Fatal(err)
	}

	docs := make([]Document, 10)
	docPointers := make([]*Document, 10)
	docIds := make([]string, 10)
	for i := 0; i < 10; i++ {
		docIds[i] = fmt.Sprintf("doc%d", i)
		docs[i] = NewDocument(docIds[i], 1).Set("foo", "Hello world")
		docPointers[i] = &docs[i]
	}
	err := c.Index(docs...)
	assert.Nil(t, err)

	type fields struct {
		pool ConnPool
		name string
	}
	type args struct {
		docId string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantDoc *Document
		wantErr bool
	}{
		{"dont-exist", fields{pool: c.pool, name: c.name}, args{"dont-exist"}, nil, false},
		{"doc1", fields{pool: c.pool, name: c.name}, args{"doc1"}, &docs[1], false},
		{"doc2", fields{pool: c.pool, name: c.name}, args{"doc2"}, &docs[2], false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			i := &Client{
				pool: tt.fields.pool,
				name: tt.fields.name,
			}
			gotDoc, err := i.Get(tt.args.docId)
			if (err != nil) != tt.wantErr {
				t.Errorf("Get() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if gotDoc != nil {
				if !reflect.DeepEqual(gotDoc, tt.wantDoc) {
					t.Errorf("Get() gotDoc = %v, want %v", gotDoc, tt.wantDoc)
				}
			}

		})
	}
}

func TestClient_MultiGet(t *testing.T) {

	c := createClient("test-get")
	c.Drop()

	sc := NewSchema(DefaultOptions).
		AddField(NewTextField("foo"))

	if err := c.CreateIndex(sc); err != nil {
		t.Fatal(err)
	}

	docs := make([]Document, 10)
	docPointers := make([]*Document, 10)
	docIds := make([]string, 10)
	for i := 0; i < 10; i++ {
		docIds[i] = fmt.Sprintf("doc%d", i)
		docs[i] = NewDocument(docIds[i], 1).Set("foo", "Hello world")
		docPointers[i] = &docs[i]
	}
	err := c.Index(docs...)
	assert.Nil(t, err)

	type fields struct {
		pool ConnPool
		name string
	}
	type args struct {
		documentIds []string
	}
	tests := []struct {
		name     string
		fields   fields
		args     args
		wantDocs []*Document
		wantErr  bool
	}{
		{"dont-exist", fields{pool: c.pool, name: c.name}, args{[]string{"dont-exist"}}, []*Document{nil}, false},
		{"doc2", fields{pool: c.pool, name: c.name}, args{[]string{"doc3"}}, []*Document{&docs[3]}, false},
		{"doc1", fields{pool: c.pool, name: c.name}, args{[]string{"doc1"}}, []*Document{&docs[1]}, false},
		{"doc1-and-other-dont-exist", fields{pool: c.pool, name: c.name}, args{[]string{"doc1", "dontexist"}}, []*Document{&docs[1], nil}, false},
		{"dont-exist-and-doc1", fields{pool: c.pool, name: c.name}, args{[]string{"dontexist", "doc1"}}, []*Document{nil, &docs[1]}, false},
		{"alldocs", fields{pool: c.pool, name: c.name}, args{docIds}, docPointers, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			i := &Client{
				pool: tt.fields.pool,
				name: tt.fields.name,
			}
			gotDocs, err := i.MultiGet(tt.args.documentIds)
			if (err != nil) != tt.wantErr {
				t.Errorf("MultiGet() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(gotDocs, tt.wantDocs) {
				t.Errorf("MultiGet() gotDocs = %v, want %v", gotDocs, tt.wantDocs)
			}
		})
	}
}

func TestClient_DictAdd(t *testing.T) {
	c := createClient("test-get")
	_, err := c.pool.Get().Do("FLUSHALL")
	assert.Nil(t, err)

	type fields struct {
		pool ConnPool
		name string
	}
	type args struct {
		dictionaryName string
		terms          []string
	}
	tests := []struct {
		name         string
		fields       fields
		args         args
		wantNewTerms int
		wantErr      bool
	}{
		{"empty-error", fields{pool: c.pool, name: c.name}, args{"dict1", []string{},}, 0, true},
		{"1-term", fields{pool: c.pool, name: c.name}, args{"dict1", []string{"term1"},}, 1, false},
		{"2nd-time-term", fields{pool: c.pool, name: c.name}, args{"dict1", []string{"term1"},}, 0, false},
		{"multi-term", fields{pool: c.pool, name: c.name}, args{"dict1", []string{"t1", "t2", "t3", "t4", "t5"},}, 5, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			i := &Client{
				pool: tt.fields.pool,
				name: tt.fields.name,
			}
			gotNewTerms, err := i.DictAdd(tt.args.dictionaryName, tt.args.terms)
			if (err != nil) != tt.wantErr {
				t.Errorf("DictAdd() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if gotNewTerms != tt.wantNewTerms {
				t.Errorf("DictAdd() gotNewTerms = %v, want %v", gotNewTerms, tt.wantNewTerms)
			}
		})
	}
}

func TestClient_DictDel(t *testing.T) {

	c := createClient("test-get")
	_, err := c.pool.Get().Do("FLUSHALL")
	assert.Nil(t, err)

	terms := make([]string, 10)
	for i := 0; i < 10; i++ {
		terms[i] = fmt.Sprintf("term%d", i)
	}

	c.DictAdd("dict1", terms)

	type fields struct {
		pool ConnPool
		name string
	}
	type args struct {
		dictionaryName string
		terms          []string
	}
	tests := []struct {
		name             string
		fields           fields
		args             args
		wantDeletedTerms int
		wantErr          bool
	}{
		{"empty-error", fields{pool: c.pool, name: c.name}, args{"dict1", []string{},}, 0, true},
		{"1-term", fields{pool: c.pool, name: c.name}, args{"dict1", []string{"term1"},}, 1, false},
		{"2nd-time-term", fields{pool: c.pool, name: c.name}, args{"dict1", []string{"term1"},}, 0, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			i := &Client{
				pool: tt.fields.pool,
				name: tt.fields.name,
			}
			gotDeletedTerms, err := i.DictDel(tt.args.dictionaryName, tt.args.terms)
			if (err != nil) != tt.wantErr {
				t.Errorf("DictDel() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if gotDeletedTerms != tt.wantDeletedTerms {
				t.Errorf("DictDel() gotDeletedTerms = %v, want %v", gotDeletedTerms, tt.wantDeletedTerms)
			}
		})
	}
}

func TestClient_DictDump(t *testing.T) {
	c := createClient("test-get")
	_, err := c.pool.Get().Do("FLUSHALL")
	assert.Nil(t, err)

	terms1 := make([]string, 10)
	for i := 0; i < 10; i++ {
		terms1[i] = fmt.Sprintf("term%d", i)
	}
	c.DictAdd("dict1", terms1)

	type fields struct {
		pool ConnPool
		name string
	}
	type args struct {
		dictionaryName string
	}
	tests := []struct {
		name      string
		fields    fields
		args      args
		wantTerms []string
		wantErr   bool
	}{
		{"empty-error", fields{pool: c.pool, name: c.name}, args{"dontexist"}, []string{}, true},
		{"dict1", fields{pool: c.pool, name: c.name}, args{"dict1"}, terms1, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			i := &Client{
				pool: tt.fields.pool,
				name: tt.fields.name,
			}
			gotTerms, err := i.DictDump(tt.args.dictionaryName)
			if (err != nil) != tt.wantErr {
				t.Errorf("DictDump() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(gotTerms, tt.wantTerms) && !tt.wantErr {
				t.Errorf("DictDump() gotTerms = %v, want %v", gotTerms, tt.wantTerms)
			}
		})
	}
}

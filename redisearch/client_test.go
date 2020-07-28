package redisearch

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/gomodule/redigo/redis"
	"github.com/stretchr/testify/assert"
)

func flush(c *Client) (err error) {
	conn := c.pool.Get()
	defer conn.Close()
	return conn.Send("FLUSHALL")
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
		docIds[i] = fmt.Sprintf("doc-get-%d", i)
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
		{"doc-get-1", fields{pool: c.pool, name: c.name}, args{"doc-get-1"}, &docs[1], false},
		{"doc-get-2", fields{pool: c.pool, name: c.name}, args{"doc-get-2"}, &docs[2], false},
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
		docIds[i] = fmt.Sprintf("doc-get-%d", i)
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
		{"doc2", fields{pool: c.pool, name: c.name}, args{[]string{"doc-get-3"}}, []*Document{&docs[3]}, false},
		{"doc1", fields{pool: c.pool, name: c.name}, args{[]string{"doc-get-1"}}, []*Document{&docs[1]}, false},
		{"doc1-and-other-dont-exist", fields{pool: c.pool, name: c.name}, args{[]string{"doc-get-1", "dontexist"}}, []*Document{&docs[1], nil}, false},
		{"dont-exist-and-doc1", fields{pool: c.pool, name: c.name}, args{[]string{"dontexist", "doc-get-1"}}, []*Document{nil, &docs[1]}, false},
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
	c := createClient("TestClient_DictAdd_Index")
	// dict tests require flushall
	flush(c)

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
		{"empty-error", fields{pool: c.pool, name: c.name}, args{"dict1", []string{}}, 0, true},
		{"1-term", fields{pool: c.pool, name: c.name}, args{"dict1", []string{"term1"}}, 1, false},
		{"2nd-time-term", fields{pool: c.pool, name: c.name}, args{"dict1", []string{"term1","term1"}}, 1, false},
		{"multi-term", fields{pool: c.pool, name: c.name}, args{"dict-multi-term", []string{"t1", "t2", "t3", "t4", "t5"}}, 5, false},
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
			i.DictDel(tt.args.dictionaryName, tt.args.terms)
		})
	}
}

func TestClient_DictDel(t *testing.T) {

	c := createClient("TestClient_DictDel_Index")
	// dict tests require flushall
	flush(c)

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
		{"empty-error", fields{pool: c.pool, name: c.name}, args{"dict1", []string{}}, 0, true},
		{"1-term", fields{pool: c.pool, name: c.name}, args{"dict1", []string{"term1"}}, 1, false},
		{"2nd-time-term", fields{pool: c.pool, name: c.name}, args{"dict1", []string{"term1"}}, 0, false},
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
	c := createClient("TestClient_DictDump_Index")
	// dict tests require flushall
	flush(c)

	terms1 := make([]string, 10)
	for i := 0; i < 10; i++ {
		terms1[i] = fmt.Sprintf("term%d", i)
	}
	c.DictAdd("dictdump-dict1", terms1)

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
		{"dictdump-dict1", fields{pool: c.pool, name: c.name}, args{"dictdump-dict1"}, terms1, false},
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

func TestClient_AliasAdd(t *testing.T) {
	c := createClient("testalias")
	c1_unexistingIndex := createClient("testaliasadd-dontexist")

	sc := NewSchema(DefaultOptions).
		AddField(NewTextField("foo")).
		AddField(NewTextField("bar"))
	c.Drop()
	assert.Nil(t, c.CreateIndex(sc))

	docs := make([]Document, 100)
	for i := 0; i < 100; i++ {
		docs[i] = NewDocument(fmt.Sprintf("doc--alias-add-%d", i), 1).Set("foo", "hello world").Set("bar", "hello world foo bar baz")
	}
	err := c.Index(docs...)

	assert.Nil(t, err)

	type fields struct {
		pool ConnPool
		name string
	}
	type args struct {
		name string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{"unexisting-index", fields{pool: c1_unexistingIndex.pool, name: c1_unexistingIndex.name}, args{"dont-exist"}, true},
		{"alias-ok", fields{pool: c.pool, name: c.name}, args{"testalias"}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			i := &Client{
				pool: tt.fields.pool,
				name: tt.fields.name,
			}
			if err := i.AliasAdd(tt.args.name); (err != nil) != tt.wantErr {
				t.Errorf("AliasAdd() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestClient_AliasDel(t *testing.T) {
	c := createClient("testaliasdel")
	c1_unexistingIndex := createClient("testaliasdel-dontexist")

	sc := NewSchema(DefaultOptions).
		AddField(NewTextField("foo")).
		AddField(NewTextField("bar"))
	c.Drop()
	err := c.CreateIndex(sc)
	assert.Nil(t, err)

	docs := make([]Document, 100)
	for i := 0; i < 100; i++ {
		docs[i] = NewDocument(fmt.Sprintf("doc-alias-del-%d", i), 1).Set("foo", "hello world").Set("bar", "hello world foo bar baz")
	}
	err = c.Index(docs...)

	assert.Nil(t, err)
	err = c.AliasAdd("aliasdel1")
	assert.Nil(t, err)

	type fields struct {
		pool ConnPool
		name string
	}
	type args struct {
		name string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{"unexisting-index", fields{pool: c1_unexistingIndex.pool, name: c1_unexistingIndex.name}, args{"dont-exist"}, true},
		{"aliasdel1", fields{pool: c.pool, name: c.name}, args{"aliasdel1"}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			i := &Client{
				pool: tt.fields.pool,
				name: tt.fields.name,
			}
			if err := i.AliasDel(tt.args.name); (err != nil) != tt.wantErr {
				t.Errorf("AliasDel() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestClient_AliasUpdate(t *testing.T) {
	c := createClient("testaliasupdateindex")

	sc := NewSchema(DefaultOptions).
		AddField(NewTextField("foo")).
		AddField(NewTextField("bar"))
	c.Drop()
	err := c.CreateIndex(sc)
	assert.Nil(t, err)

	docs := make([]Document, 100)
	for i := 0; i < 100; i++ {
		docs[i] = NewDocument(fmt.Sprintf("doc-alias-update-%d", i), 1).Set("foo", "hello world").Set("bar", "hello world foo bar baz")
	}
	err = c.Index(docs...)

	assert.Nil(t, err)
	err = c.AliasAdd("aliasupdate")
	assert.Nil(t, err)
	type fields struct {
		pool ConnPool
		name string
	}
	type args struct {
		name string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{"aliasupdate", fields{pool: c.pool, name: c.name}, args{"aliasupdate"}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			i := &Client{
				pool: tt.fields.pool,
				name: tt.fields.name,
			}
			if err := i.AliasUpdate(tt.args.name); (err != nil) != tt.wantErr {
				t.Errorf("AliasUpdate() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestClient_Config(t *testing.T) {
	c := createClient("testconfigindex")
	c.Drop()
	ret, err := c.SetConfig("TIMEOUT", "100")
	assert.Nil(t, err)
	assert.Equal(t, "OK", ret)

	var kvs map[string]string
	kvs, _ = c.GetConfig("TIMEOUT")
	assert.Equal(t, "100", kvs["TIMEOUT"])

	kvs, _ = c.GetConfig("*")
	assert.Equal(t, "100", kvs["TIMEOUT"])
}

func TestNewClientFromPool(t *testing.T) {
	host, password := getTestConnectionDetails()
	pool := &redis.Pool{Dial: func() (redis.Conn, error) {
		return redis.Dial("tcp", host, redis.DialPassword(password))
	}, MaxIdle: maxConns}
	client1 := NewClientFromPool(pool, "index1")
	client2 := NewClientFromPool(pool, "index2")
	assert.Equal(t, client1.pool, client2.pool)
	err1 := client1.pool.Close()
	err2 := client2.pool.Close()
	assert.Nil(t, err1)
	assert.Nil(t, err2)
}

func TestClient_GetTagVals(t *testing.T) {
	c := createClient("testgettagvals")

	// Create a schema
	sc := NewSchema(DefaultOptions).
		AddField(NewTextField("name")).
		AddField(NewTagField("tags"))

	c.Drop()
	c.CreateIndex(sc)

	docs := make([]Document, 1)
	doc := NewDocument("doc1", 1.0)
	doc.Set("name", "John").
		Set("tags", "single, young")
	docs[0] = doc
	c.Index(docs...)
	tags, err := c.GetTagVals("testgettagvals", "tags")
	assert.Nil(t, err)
	assert.Contains(t, tags, "single")
	// negative tests
	tags, err = c.GetTagVals("notexit", "tags")
	assert.NotNil(t, err)
	assert.Nil(t, tags)
}

func TestClient_SynAdd(t *testing.T) {
	c := createClient("testsynadd")

	sc := NewSchema(DefaultOptions).
		AddField(NewTextField("name")).
		AddField(NewTextField("addr"))
	c.Drop()
	err := c.CreateIndex(sc)
	assert.Nil(t, err)

	gid, err := c.SynAdd("testsynadd", []string{"girl", "baby"})
	assert.Nil(t, err)
	assert.True(t, gid >= 0)
	ret, err := c.SynUpdate("testsynadd", gid, []string{"girl", "baby"})
	assert.Nil(t, err)
	assert.Equal(t, "OK", ret)
}

func TestClient_SynDump(t *testing.T) {
	c := createClient("testsyndump")

	sc := NewSchema(DefaultOptions).
		AddField(NewTextField("name")).
		AddField(NewTextField("addr"))
	c.Drop()
	err := c.CreateIndex(sc)
	assert.Nil(t, err)

	gid, err := c.SynAdd("testsyndump", []string{"girl", "baby"})
	assert.Nil(t, err)
	assert.True(t, gid >= 0)

	gid2, err := c.SynAdd("testsyndump", []string{"child"})

	m, err := c.SynDump("testsyndump")
	assert.Contains(t, m, "baby")
	assert.Contains(t, m, "girl")
	assert.Contains(t, m, "child")
	assert.Equal(t, gid, m["baby"][0])
	assert.Equal(t, gid2, m["child"][0])
}

func TestClient_AddHash(t *testing.T) {
	c := createClient("testAddHash")

	sc := NewSchema(DefaultOptions).
		AddField(NewTextField("name")).
		AddField(NewTextField("addr"))
	c.Drop()
	err := c.CreateIndex(sc)
	assert.Nil(t, err)

	// Add a hash key
	c.pool.Get().Do("HMSET", "myhash", "field1", "Hello")

	ret, err := c.AddHash("myhash", 1, "english", false)
	// Given that FT.ADDHASH is no longer valid for search2+ we assert it's error
	if err != nil {
		assert.Equal(t, "ERR unknown command `FT.ADDHASH`, with args beginning with: `testAddHash`, `myhash`, `1`, `LANGUAGE`, `english`, ", err.Error())
	} else {
		assert.Equal(t, "OK", ret)
	}
}

func TestClient_AddField(t *testing.T) {
	c := createClient("alterTest")
	sc := NewSchema(DefaultOptions).
		AddField(NewTextField("name")).
		AddField(NewTextField("addr"))
	c.Drop()
	err := c.CreateIndex(sc)
	assert.Nil(t, err)
	err = c.AddField(NewNumericField("age"))
	assert.Nil(t, err)
	err = c.Index(NewDocument("doc-n1",1.0).Set("age",15 ))
	assert.Nil(t, err)
}

func TestClient_CreateIndex(t *testing.T) {
	type fields struct {
		pool ConnPool
		name string
	}
	type args struct {
		s *Schema
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			i := &Client{
				pool: tt.fields.pool,
				name: tt.fields.name,
			}
			if err := i.CreateIndex(tt.args.s); (err != nil) != tt.wantErr {
				t.Errorf("CreateIndex() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
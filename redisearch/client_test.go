package redisearch

import (
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/gomodule/redigo/redis"
	"github.com/stretchr/testify/assert"
)

func flush(c *Client) (err error) {
	conn := c.pool.Get()
	defer conn.Close()
	return conn.Send("FLUSHALL")
}

func teardown(c *Client) {
	flush(c)
}

// getRediSearchVersion returns RediSearch version by issuing "MODULE LIST" command
// and iterating through the availabe modules up until "ft" is found as the name property
func (c *Client) getRediSearchVersion() (version int64, err error) {
	conn := c.pool.Get()
	defer conn.Close()
	var values []interface{}
	var moduleInfo []interface{}
	var moduleName string
	values, err = redis.Values(conn.Do("MODULE", "LIST"))
	if err != nil {
		return
	}
	for _, rawModule := range values {
		moduleInfo, err = redis.Values(rawModule, err)
		if err != nil {
			return
		}
		moduleName, err = redis.String(moduleInfo[1], err)
		if err != nil {
			return
		}
		if moduleName == "search" {
			version, err = redis.Int64(moduleInfo[3], err)
		}
	}
	return
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
	teardown(c)
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
	teardown(c)
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
		{"2nd-time-term", fields{pool: c.pool, name: c.name}, args{"dict1", []string{"term1", "term1"}}, 1, false},
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
	teardown(c)
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
	teardown(c)
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
	teardown(c)
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
	teardown(c)
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
	teardown(c)
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
	teardown(c)
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
	teardown(c)
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
	teardown(client1)
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
	teardown(c)
}

func TestClient_SynAdd(t *testing.T) {
	c := createClient("testsynadd")
	version, err := c.getRediSearchVersion()
	assert.Nil(t, err)
	if version <= 10699 {
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
	teardown(c)
}

func TestClient_SynDump(t *testing.T) {
	c := createClient("testsyndump")
	version, err := c.getRediSearchVersion()
	assert.Nil(t, err)
	sc := NewSchema(DefaultOptions).
		AddField(NewTextField("name")).
		AddField(NewTextField("addr"))
	c.Drop()
	err = c.CreateIndex(sc)
	var gId1 int64 = 1
	var gId2 int64 = 2

	assert.Nil(t, err)
	// For RediSearch < v2.0 we need to use SYNADD. For Redisearch >= v2.0 we need to use SYNUPDATE
	if version <= 10699 {
		gId1, err = c.SynAdd("testsyndump", []string{"girl", "baby"})
		assert.Nil(t, err)
		gId2, err = c.SynAdd("testsyndump", []string{"child"})
		assert.Nil(t, err)
	} else {
		ret, err := c.SynUpdate("testsyndump", gId1, []string{"girl", "baby"})
		assert.Nil(t, err)
		assert.Equal(t, "OK", ret)
		_, err = c.SynUpdate("testsyndump", gId2, []string{"child"})
		assert.Nil(t, err)
		assert.Equal(t, "OK", ret)
	}

	m, err := c.SynDump("testsyndump")
	assert.Contains(t, m, "baby")
	assert.Contains(t, m, "girl")
	assert.Contains(t, m, "child")
	teardown(c)
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
	teardown(c)
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
	err = c.Index(NewDocument("doc-n1", 1.0).Set("age", 15))
	assert.Nil(t, err)
	teardown(c)
}

func TestClient_GetRediSearchVersion(t *testing.T) {
	c := createClient("version-test")
	_, err := c.getRediSearchVersion()
	assert.Nil(t, err)
}

func TestClient_CreateIndexWithIndexDefinitionJSON(t *testing.T) {
	c := createClient("index-definition-test")
	version, err := c.getRediSearchVersion()
	assert.Nil(t, err)
	if version <= 20200 {
		// JSON IndexDefinition is available for RediSearch 2.2+
		return
	}
	// Create a schema
	sc := NewSchema(DefaultOptions).
		AddField(NewTextFieldOptions("name", TextFieldOptions{Sortable: true})).
		AddField(NewTextFieldOptions("description", TextFieldOptions{Weight: 5.0, Sortable: true})).
		AddField(NewNumericField("price"))

	type args struct {
		schema     *Schema
		definition *IndexDefinition
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{"default+index_on", args{sc, NewIndexDefinition().SetIndexOn(JSON)}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c.Drop()
			if err := c.CreateIndexWithIndexDefinition(tt.args.schema, tt.args.definition); (err != nil) != tt.wantErr {
				t.Errorf("CreateIndexWithIndexDefinition() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
		teardown(c)

	}
}

func TestClient_CreateIndexWithIndexDefinition(t *testing.T) {
	i := createClient("index-definition-test")
	version, err := i.getRediSearchVersion()
	assert.Nil(t, err)
	if version >= 20000 {

		type args struct {
			schema     *Schema
			definition *IndexDefinition
		}
		tests := []struct {
			name    string
			args    args
			wantErr bool
		}{
			{"no-indexDefinition", args{NewSchema(DefaultOptions).
				AddField(NewTextField("name")).
				AddField(NewTextField("addr")), nil}, false},
			{"default-indexDefinition", args{NewSchema(DefaultOptions).
				AddField(NewTextField("name")).
				AddField(NewTextField("addr")), NewIndexDefinition()}, false},
			{"score-indexDefinition", args{NewSchema(DefaultOptions).
				AddField(NewTextField("name")).
				AddField(NewTextField("addr")), NewIndexDefinition().SetScore(0.25)}, false},
			{"language-indexDefinition", args{NewSchema(DefaultOptions).
				AddField(NewTextField("name")).
				AddField(NewTextField("addr")), NewIndexDefinition().SetLanguage("portuguese")}, false},
			{"language_field-indexDefinition", args{NewSchema(DefaultOptions).
				AddField(NewTextField("name")).
				AddField(NewTextField("lang")).
				AddField(NewTextField("addr")), NewIndexDefinition().SetLanguageField("lang")}, false},
			{"score_field-indexDefinition", args{NewSchema(DefaultOptions).
				AddField(NewTextField("name")).
				AddField(NewTextField("addr")).AddField(NewNumericField("score")), NewIndexDefinition().SetScoreField("score")}, false},
			{"payload_field-indexDefinition", args{NewSchema(DefaultOptions).
				AddField(NewTextField("name")).
				AddField(NewTextField("addr")).AddField(NewNumericField("score")).AddField(NewTextField("payload")), NewIndexDefinition().SetPayloadField("payload")}, false},
			{"prefix-indexDefinition", args{NewSchema(DefaultOptions).
				AddField(NewTextField("name")).
				AddField(NewTextField("addr")).AddField(NewNumericField("score")).AddField(NewTextField("payload")), NewIndexDefinition().AddPrefix("doc:*")}, false},
			{"filter-indexDefinition", args{NewSchema(DefaultOptions).
				AddField(NewTextField("name")).
				AddField(NewTextField("addr")).AddField(NewNumericField("score")).AddField(NewTextField("payload")), NewIndexDefinition().SetFilterExpression("@score > 0")}, false},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				if err := i.CreateIndexWithIndexDefinition(tt.args.schema, tt.args.definition); (err != nil) != tt.wantErr {
					t.Errorf("CreateIndexWithIndexDefinition() error = %v, wantErr %v", err, tt.wantErr)
				}
				teardown(i)
			})
		}
	}
}

func TestClient_SynUpdate(t *testing.T) {
	c := createClient("syn-update-test")
	sc := NewSchema(DefaultOptions).
		AddField(NewTextField("name")).
		AddField(NewTextField("addr"))
	version, err := c.getRediSearchVersion()
	assert.Nil(t, err)

	type args struct {
		indexName      string
		synonymGroupId int64
		terms          []string
	}
	tests := []struct {
		name    string
		args    args
		want    string
		wantErr bool
	}{
		{"1-syn", args{"syn-update-test", 1, []string{"abc"}}, "OK", false},
		{"3-syn", args{"syn-update-test", 1, []string{"abc", "def", "ghi"}}, "OK", false},
		{"err-empty-syn", args{"syn-update-test", 1, []string{}}, "", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c.Drop()
			err := c.CreateIndex(sc)
			assert.Nil(t, err)
			gId := tt.args.synonymGroupId

			// For older version of RediSearch we first need to use SYNADD then SYNUPDATE
			if version <= 10699 {
				gId, err = c.SynAdd(tt.args.indexName, []string{"workaround"})
				assert.Nil(t, err)
			}

			got, err := c.SynUpdate(tt.args.indexName, gId, tt.args.terms)
			if (err != nil) != tt.wantErr {
				t.Errorf("SynUpdate() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("SynUpdate() got = %v, want %v", got, tt.want)
			}
			teardown(c)
		})
	}
}

func TestClient_Delete(t *testing.T) {
	c := createClient("ft.del-test")
	sc := NewSchema(DefaultOptions).
		AddField(NewTextField("name")).
		AddField(NewTextField("addr"))
	version, err := c.getRediSearchVersion()
	assert.Nil(t, err)

	type args struct {
		docId          string
		deleteDocument bool
	}
	tests := []struct {
		name                string
		args                args
		wantErr             bool
		documentShouldExist bool
	}{
		{"persist-doc", args{"doc1", false}, false, true},
		{"delete-doc", args{"doc1", true}, false, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c.Drop()
			err := c.CreateIndex(sc)
			assert.Nil(t, err)
			err = c.Index(NewDocument(tt.args.docId, 1.0).Set("name", "Jon Doe"))
			assert.Nil(t, err)
			if err := c.Delete(tt.args.docId, tt.args.deleteDocument); (err != nil) != tt.wantErr {
				t.Errorf("Delete() error = %v, wantErr %v", err, tt.wantErr)
			}
			docExists, err := redis.Bool(c.pool.Get().Do("EXISTS", tt.args.docId))
			assert.Nil(t, err)
			if version <= 10699 {
				assert.Equal(t, tt.documentShouldExist, docExists)
			} else {
				assert.Equal(t, false, docExists)
			}
			teardown(c)
		})
	}
}

func TestClient_DeleteDocument(t *testing.T) {
	c := createClient("ft.DeleteDocument-test")
	sc := NewSchema(DefaultOptions).
		AddField(NewTextField("name")).
		AddField(NewTextField("addr"))

	type args struct {
		docId          string
		docIdsToAddIdx []string
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{"doc-exists", args{"doc1", []string{"doc1", "doc2"}}, false},
		{"doc-not-exists", args{"doc3", []string{"doc1", "doc2"}}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c.Drop()
			err := c.CreateIndex(sc)
			assert.Nil(t, err)
			for _, docId := range tt.args.docIdsToAddIdx {
				err = c.Index(NewDocument(docId, 1.0).Set("name", "Jon Doe"))
				assert.Nil(t, err)
			}
			if err := c.DeleteDocument(tt.args.docId); (err != nil) != tt.wantErr {
				t.Errorf("DeleteDocument() error = %v, wantErr %v", err, tt.wantErr)
			}
			docExists, err := redis.Bool(c.pool.Get().Do("EXISTS", tt.args.docId))
			assert.Nil(t, err)
			assert.False(t, docExists)
			teardown(c)
		})
	}
}

func TestClient_CreateIndexWithIndexDefinition1(t *testing.T) {
	c := createClient("index-definition-test")
	version, err := c.getRediSearchVersion()
	assert.Nil(t, err)
	if version <= 10699 {
		// IndexDefinition is available for RediSearch 2.0+
		return
	}
	// Create a schema
	sc := NewSchema(DefaultOptions).
		AddField(NewTextFieldOptions("name", TextFieldOptions{Sortable: true})).
		AddField(NewTextFieldOptions("description", TextFieldOptions{Weight: 5.0, Sortable: true})).
		AddField(NewNumericField("price"))

	type args struct {
		schema     *Schema
		definition *IndexDefinition
	}
	tests := []struct {
		name    string
		args    args
		wantErr bool
	}{
		{"default", args{sc, NewIndexDefinition()}, false},
		{"default+async", args{sc, NewIndexDefinition().SetAsync(true)}, false},
		{"default+score", args{sc, NewIndexDefinition().SetScore(0.75)}, false},
		{"default+score_field", args{sc, NewIndexDefinition().SetScoreField("myscore")}, false},
		{"default+language", args{sc, NewIndexDefinition().SetLanguage("portuguese")}, false},
		{"default+language_field", args{sc, NewIndexDefinition().SetLanguageField("mylanguage")}, false},
		{"default+prefix", args{sc, NewIndexDefinition().AddPrefix("products:*")}, false},
		{"default+payload_field", args{sc, NewIndexDefinition().SetPayloadField("products_description")}, false},
		{"default+filter", args{sc, NewIndexDefinition().SetFilterExpression("@score >= 0")}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c.Drop()
			if err := c.CreateIndexWithIndexDefinition(tt.args.schema, tt.args.definition); (err != nil) != tt.wantErr {
				t.Errorf("CreateIndexWithIndexDefinition() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
		teardown(c)

	}
}

func TestClient_CreateIndex(t *testing.T) {
	c := createClient("create-index-info")
	flush(c)
	version, err := c.getRediSearchVersion()
	assert.Nil(t, err)
	if version <= 10699 {
		// IndexDefinition is available for RediSearch 2.0+
		return
	}

	// Create a schema
	schema := NewSchema(DefaultOptions).
		AddField(NewTextFieldOptions("name", TextFieldOptions{Sortable: true, PhoneticMatcher: PhoneticDoubleMetaphoneEnglish})).
		AddField(NewNumericField("age"))

	// IndexDefinition is available for RediSearch 2.0+
	// In this example we will only index keys started by product:
	indexDefinition := NewIndexDefinition().AddPrefix("create-index-info:")

	// Add the Index Definition
	c.CreateIndexWithIndexDefinition(schema, indexDefinition)
	assert.Nil(t, err)

	// Create docs with a name that has the same phonetic matcher
	vanillaConnection := c.pool.Get()
	_, err = vanillaConnection.Do("HSET", "create-index-info:doc1", "name", "Jon", "age", 25)
	assert.Nil(t, err)
	_, err = vanillaConnection.Do("HSET", "create-index-info:doc2", "name", "John", "age", 20)
	assert.Nil(t, err)

	// Wait for all documents to be indexed
	info, err := c.Info()
	assert.Nil(t, err)
	for info.IsIndexing {
		time.Sleep(time.Second)
		info, _ = c.Info()
	}
	assert.Equal(t, uint64(2), info.DocCount)
	assert.Equal(t, false, info.IsIndexing)
	assert.Equal(t, uint64(0), info.HashIndexingFailures)
	docs, total, err := c.Search(NewQuery("Jon").
		SetReturnFields("name"))
	assert.Nil(t, err)
	// Verify that the we've received 2 documents ( Jon and John )
	assert.Equal(t, 2, total)
	assert.Equal(t, "Jon", docs[0].Properties["name"])
	assert.Equal(t, "John", docs[1].Properties["name"])
}

func TestClient_CreateJsonIndex(t *testing.T) {
	c := createClient("create-json-index")
	flush(c)
	version, _ := c.getRediSearchVersion()
	if version < 20200 {
		// IndexDefinition is available for RediSearch 2.0+
		return
	}

	// Create a schema
	schema := NewSchema(DefaultOptions).
		AddField(NewTextFieldOptions("$.name", TextFieldOptions{Sortable: true, PhoneticMatcher: PhoneticDoubleMetaphoneEnglish, As: "name"})).
		AddField(NewNumericFieldOptions("$.age", NumericFieldOptions{As: "age"}))

	// IndexDefinition is available for RediSearch 2.0+
	// In this example we will only index keys started by product:
	indexDefinition := NewIndexDefinition().SetIndexOn(JSON).AddPrefix("create-json-index:")

	// Add the Index Definition
	err := c.CreateIndexWithIndexDefinition(schema, indexDefinition)
	assert.Nil(t, err)

	// Create docs with a name that has the same phonetic matcher
	vanillaConnection := c.pool.Get()
	_, err = vanillaConnection.Do("JSON.SET", "create-json-index:doc1", "$", "{\"name\":\"Jon\", \"age\": 25}")
	assert.Nil(t, err)
	_, err = vanillaConnection.Do("JSON.SET", "create-json-index:doc2", "$", "{\"name\":\"John\", \"age\": 25}")
	assert.Nil(t, err)

	// Wait for all documents to be indexed
	info, err := c.Info()
	assert.Nil(t, err)
	for info.IsIndexing {
		time.Sleep(time.Second)
		info, _ = c.Info()
	}

	assert.Equal(t, uint64(2), info.DocCount)
	assert.Equal(t, false, info.IsIndexing)
	assert.Equal(t, uint64(0), info.HashIndexingFailures)
	docs, total, err := c.Search(NewQuery("Jon").
		SetReturnFields("name"))
	assert.Nil(t, err)
	// Verify that the we've received 2 documents ( Jon and John )
	assert.Equal(t, 2, total)
	assert.Equal(t, "Jon", docs[0].Properties["name"])
	assert.Equal(t, "John", docs[1].Properties["name"])
}

func TestClient_CreateIndex_failure(t *testing.T) {
	c := createClient("create-index-failure")
	flush(c)
	version, err := c.getRediSearchVersion()
	assert.Nil(t, err)
	if version <= 10699 {
		// IndexDefinition is available for RediSearch 2.0+
		return
	}
	c.DropIndex(true)

	// Create a schema
	schema := NewSchema(DefaultOptions).
		AddField(NewTextFieldOptions("name", TextFieldOptions{Sortable: true, PhoneticMatcher: PhoneticDoubleMetaphoneEnglish})).
		AddField(NewNumericFieldOptions("age", NumericFieldOptions{Sortable: true}))

	// IndexDefinition is available for RediSearch 2.0+
	// In this example we will only index keys started by product:
	indexDefinition := NewIndexDefinition().AddPrefix("create-index-failure:")

	// Add the Index Definition
	c.CreateIndexWithIndexDefinition(schema, indexDefinition)
	assert.Nil(t, err)

	// Create docs with a name that has the same phonetic matcher
	vanillaConnection := c.pool.Get()
	vanillaConnection.Do("HSET", "create-index-failure:doc1", "name", "Jon", "age", "abc")
	vanillaConnection.Do("HSET", "create-index-failure:doc2", "name", "John", "age", 20)

	// Wait for all documents to be indexed
	info, _ := c.Info()
	for info.IsIndexing {
		time.Sleep(time.Second)
		info, _ = c.Info()
	}
	assert.Equal(t, uint64(1), info.DocCount)
	assert.Equal(t, false, info.IsIndexing)
	assert.Equal(t, uint64(1), info.HashIndexingFailures)
	docs, total, err := c.Search(NewQuery("Jon").
		SetReturnFields("name"))
	assert.Nil(t, err)
	// Verify that the we've received 1 document ( John )
	assert.Equal(t, 1, total)
	assert.Equal(t, "John", docs[0].Properties["name"])

	// Drop index but keep docs
	err = c.DropIndex(true)
	assert.Nil(t, err)
}

func TestClient_DropIndex(t *testing.T) {
	c := createClient("drop-index-example")
	version, err := c.getRediSearchVersion()
	assert.Nil(t, err)
	if version <= 10699 {
		// DropIndex() is available for RediSearch 2.0+
		return
	}

	// Create a schema
	schema := NewSchema(DefaultOptions).
		AddField(NewTextFieldOptions("name", TextFieldOptions{Sortable: true, PhoneticMatcher: PhoneticDoubleMetaphoneEnglish})).
		AddField(NewNumericField("age"))

	// IndexDefinition is available for RediSearch 2.0+
	// In this example we will only index keys started by product:
	indexDefinition := NewIndexDefinition().AddPrefix("drop-index:")

	// Add the Index Definition
	err = c.CreateIndexWithIndexDefinition(schema, indexDefinition)
	assert.Nil(t, err)

	// Create docs with a name that has the same phonetic matcher
	vanillaConnection := c.pool.Get()
	vanillaConnection.Do("HSET", "drop-index:doc1", "name", "Jon", "age", 25)
	vanillaConnection.Do("HSET", "drop-index:doc2", "name", "John", "age", 20)

	// Wait for all documents to be indexed
	info, _ := c.Info()
	for info.IsIndexing {
		time.Sleep(time.Second)
		info, _ = c.Info()
	}

	// Drop index but keep docs
	err = c.DropIndex(false)
	assert.Nil(t, err)
	// Now that we don't have the index this should raise an error
	_, err = c.Info()
	assert.EqualError(t, err, "Unknown Index name")
	// Assert hashes still exist
	result, err := vanillaConnection.Do("EXISTS", "drop-index:doc1")
	assert.Equal(t, int64(1), result)
	result, err = vanillaConnection.Do("EXISTS", "drop-index:doc2")
	assert.Equal(t, int64(1), result)

	// Create index again
	err = c.CreateIndexWithIndexDefinition(schema, indexDefinition)

	// Wait for all documents to be indexed again
	info, _ = c.Info()
	for info.IsIndexing {
		time.Sleep(time.Second)
		info, _ = c.Info()
	}

	assert.Nil(t, err)
	// Drop index but keep docs
	err = c.DropIndex(true)
	assert.Nil(t, err)
	// Now that we don't have the index this should raise an error
	_, err = c.Info()
	assert.EqualError(t, err, "Unknown Index name")
	// Assert hashes still exist
	result, err = vanillaConnection.Do("EXISTS", "drop-index:doc1")
	assert.Equal(t, int64(0), result)
	result, err = vanillaConnection.Do("EXISTS", "drop-index:doc2")
	assert.Equal(t, int64(0), result)

}

func TestClient_ListIndex(t *testing.T) {
	c := createClient("index-list-test")
	flush(c)
	version, err := c.getRediSearchVersion()
	assert.Nil(t, err)
	if version <= 10699 {
		// IndexDefinition is available for RediSearch 2.0+
		return
	}
	// Create a schema
	schema := NewSchema(DefaultOptions).
		AddField(NewTextFieldOptions("name", TextFieldOptions{Sortable: true, PhoneticMatcher: PhoneticDoubleMetaphoneEnglish})).
		AddField(NewNumericField("age"))

	// IndexDefinition is available for RediSearch 2.0+
	// In this example we will only index keys started by product:
	indexDefinition := NewIndexDefinition().AddPrefix("index-list-test:")

	// Add the Index Definition
	c.CreateIndexWithIndexDefinition(schema, indexDefinition)
	assert.Nil(t, err)

	indexes, err := c.List()
	assert.Nil(t, err)
	assert.Equal(t, "index-list-test", indexes[0])
}

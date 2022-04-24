package redisearch

import (
	"math"
	"reflect"
	"testing"

	"github.com/gomodule/redigo/redis"
	"github.com/stretchr/testify/assert"
)

func TestPaging_serialize(t *testing.T) {
	type fields struct {
		Offset int
		Num    int
	}
	tests := []struct {
		name   string
		fields fields
		want   redis.Args
	}{
		{"default", fields{0, 10}, redis.Args{}},
		{"0-1000", fields{0, 1000}, redis.Args{"LIMIT", 0, 1000}},
		{"0-2", fields{0, 2}, redis.Args{"LIMIT", 0, 2}},
		{"100-10", fields{100, 10}, redis.Args{"LIMIT", 100, 10}},
		{"100-200", fields{100, 200}, redis.Args{"LIMIT", 100, 200}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := Paging{
				Offset: tt.fields.Offset,
				Num:    tt.fields.Num,
			}
			if got := p.serialize(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("serialize() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_serializeIndexingOptions(t *testing.T) {
	type args struct {
		opts IndexingOptions
		args redis.Args
	}
	tests := []struct {
		name string
		args args
		want redis.Args
	}{
		{"default with args", args{DefaultIndexingOptions, redis.Args{"idx1", "doc1", 1.0}}, redis.Args{"idx1", "doc1", 1.0}},
		{"default", args{DefaultIndexingOptions, redis.Args{}}, redis.Args{}},
		{"default + language", args{IndexingOptions{Language: "portuguese"}, redis.Args{}}, redis.Args{"LANGUAGE", "portuguese"}},
		{"replace full doc", args{IndexingOptions{Replace: true}, redis.Args{}}, redis.Args{"REPLACE"}},
		{"replace partial", args{IndexingOptions{Replace: true, Partial: true}, redis.Args{}}, redis.Args{"REPLACE", "PARTIAL"}},
		{"replace if", args{IndexingOptions{Replace: true, ReplaceCondition: "@timestamp < 23323234234"}, redis.Args{}}, redis.Args{"REPLACE", "IF", "@timestamp < 23323234234"}},
		{"replace partial if", args{IndexingOptions{Replace: true, Partial: true, ReplaceCondition: "@timestamp < 23323234234"}, redis.Args{}}, redis.Args{"REPLACE", "PARTIAL", "IF", "@timestamp < 23323234234"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := SerializeIndexingOptions(tt.args.opts, tt.args.args); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("serializeIndexingOptions() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestQuery_serialize(t *testing.T) {
	var raw = "test_query"
	type fields struct {
		Raw           string
		Flags         Flag
		InKeys        []string
		InFields      []string
		ReturnFields  []string
		Language      string
		Expander      string
		Scorer        string
		SortBy        *SortingKey
		HighlightOpts *HighlightOptions
		SummarizeOpts *SummaryOptions
		Params        map[string]interface{}
		Dialect       int
	}
	tests := []struct {
		name   string
		fields fields
		want   redis.Args
	}{
		{"default", fields{Raw: ""}, redis.Args{"", "LIMIT", 0, 0}},
		{"Raw", fields{Raw: raw}, redis.Args{raw, "LIMIT", 0, 0}},
		{"QueryVerbatim", fields{Raw: raw, Flags: QueryVerbatim}, redis.Args{raw, "LIMIT", 0, 0, "VERBATIM"}},
		{"QueryNoContent", fields{Raw: raw, Flags: QueryNoContent}, redis.Args{raw, "LIMIT", 0, 0, "NOCONTENT"}},
		{"QueryInOrder", fields{Raw: raw, Flags: QueryInOrder}, redis.Args{raw, "LIMIT", 0, 0, "INORDER"}},
		{"QueryWithPayloads", fields{Raw: raw, Flags: QueryWithPayloads}, redis.Args{raw, "LIMIT", 0, 0, "WITHPAYLOADS"}},
		{"QueryWithScores", fields{Raw: raw, Flags: QueryWithScores}, redis.Args{raw, "LIMIT", 0, 0, "WITHSCORES"}},
		{"InKeys", fields{Raw: raw, InKeys: []string{"test_key"}}, redis.Args{raw, "LIMIT", 0, 0, "INKEYS", 1, "test_key"}},
		{"InFields", fields{Raw: raw, InFields: []string{"test_key"}}, redis.Args{raw, "LIMIT", 0, 0, "INFIELDS", 1, "test_key"}},
		{"ReturnFields", fields{Raw: raw, ReturnFields: []string{"test_field"}}, redis.Args{raw, "LIMIT", 0, 0, "RETURN", 1, "test_field"}},
		{"Language", fields{Raw: raw, Language: "chinese"}, redis.Args{raw, "LIMIT", 0, 0, "LANGUAGE", "chinese"}},
		{"Expander", fields{Raw: raw, Expander: "test_expander"}, redis.Args{raw, "LIMIT", 0, 0, "EXPANDER", "test_expander"}},
		{"Scorer", fields{Raw: raw, Scorer: "test_scorer"}, redis.Args{raw, "LIMIT", 0, 0, "SCORER", "test_scorer"}},
		{"SortBy", fields{Raw: raw, SortBy: &SortingKey{
			Field:     "test_field",
			Ascending: true}}, redis.Args{raw, "LIMIT", 0, 0, "SORTBY", "test_field", "ASC"}},
		{"HighlightOpts", fields{Raw: raw, HighlightOpts: &HighlightOptions{
			Fields: []string{"test_field"},
			Tags:   [2]string{"<tag>", "</tag>"},
		}}, redis.Args{raw, "LIMIT", 0, 0, "HIGHLIGHT", "FIELDS", 1, "test_field", "TAGS", "<tag>", "</tag>"}},
		{"SummarizeOpts", fields{Raw: raw, SummarizeOpts: &SummaryOptions{
			Fields:       []string{"test_field"},
			FragmentLen:  20,
			NumFragments: 3,
			Separator:    "...",
		}}, redis.Args{raw, "LIMIT", 0, 0, "SUMMARIZE", "FIELDS", 1, "test_field", "LEN", 20, "FRAGS", 3, "SEPARATOR", "..."}},
		{"Params", fields{Raw: raw, Params: map[string]interface{}{"min": 1}}, redis.Args{raw, "LIMIT", 0, 0, "PARAMS", 4, "min", 1}},
		{"Dialect", fields{Raw: raw, Dialect: 2}, redis.Args{raw, "LIMIT", 0, 0, "DIALECT", 2}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q := Query{
				Raw:           tt.fields.Raw,
				Flags:         tt.fields.Flags,
				InKeys:        tt.fields.InKeys,
				InFields:      tt.fields.InFields,
				ReturnFields:  tt.fields.ReturnFields,
				Language:      tt.fields.Language,
				Expander:      tt.fields.Expander,
				Scorer:        tt.fields.Scorer,
				SortBy:        tt.fields.SortBy,
				HighlightOpts: tt.fields.HighlightOpts,
				SummarizeOpts: tt.fields.SummarizeOpts,
				Params:        tt.fields.Params,
				Dialect:       tt.fields.Dialect,
			}
			if g := q.serialize(); !reflect.DeepEqual(g, tt.want) {
				t.Errorf("serialize() = %v, want %v", g, tt.want)
			}
		})
	}
}

func Test_appendNumArgs(t *testing.T) {
	type args struct {
		num     float64
		exclude bool
		args    redis.Args
	}
	tests := []struct {
		name string
		args args
		want redis.Args
	}{
		{"1 arg", args{1.0, false, redis.Args{}}, redis.Args{1.0}},
		{"2.54 excluded arg", args{2.54, true, redis.Args{}}, redis.Args{"(2.54"}},
		{"+inf", args{math.Inf(1), false, redis.Args{}}, redis.Args{"+inf"}},
		{"+inf", args{math.Inf(-1), false, redis.Args{}}, redis.Args{"-inf"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := appendNumArgs(tt.args.num, tt.args.exclude, tt.args.args); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("appendNumArgs() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestQuery_SetInKeys_InFields(t *testing.T) {
	q := NewQuery("")
	q.SetInKeys("abc")
	assert.Equal(t, q.InKeys, []string{"abc"})
	q.SetInFields("field1")
	assert.Equal(t, q.InFields, []string{"field1"})
}

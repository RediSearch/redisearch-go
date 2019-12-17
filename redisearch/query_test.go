package redisearch_test

import (
	"reflect"
	"testing"

	"github.com/RediSearch/redisearch-go/redisearch"
	"github.com/garyburd/redigo/redis"
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
		{"100-200", fields{100, 200}, redis.Args{"LIMIT", 100, 200}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := redisearch.Paging{
				Offset: tt.fields.Offset,
				Num:    tt.fields.Num,
			}
			if got := p.Serialize(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("serialize() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_serializeIndexingOptions(t *testing.T) {
	type args struct {
		opts redisearch.IndexingOptions
		args redis.Args
	}
	tests := []struct {
		name string
		args args
		want redis.Args
	}{
		{"default with args", args{redisearch.DefaultIndexingOptions, redis.Args{"idx1", "doc1", 1.0}}, redis.Args{"idx1", "doc1", 1.0}},
		{"default", args{redisearch.DefaultIndexingOptions, redis.Args{}}, redis.Args{}},
		{"replace full doc", args{redisearch.IndexingOptions{Replace: true}, redis.Args{}}, redis.Args{"REPLACE"}},
		{"replace partial", args{redisearch.IndexingOptions{Replace: true, Partial: true}, redis.Args{}}, redis.Args{"REPLACE", "PARTIAL"}},
		{"replace if", args{redisearch.IndexingOptions{Replace: true, ReplaceCondition: "@timestamp < 23323234234"}, redis.Args{}}, redis.Args{"REPLACE", "IF", "@timestamp < 23323234234"}},
		{"replace partial if", args{redisearch.IndexingOptions{Replace: true, Partial: true, ReplaceCondition: "@timestamp < 23323234234"}, redis.Args{}}, redis.Args{"REPLACE", "PARTIAL", "IF", "@timestamp < 23323234234"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := redisearch.SerializeIndexingOptions(tt.args.opts, tt.args.args); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("serializeIndexingOptions() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestQuery_serialize(t *testing.T) {
	var raw = "test_query"
	type fields struct {
		Raw           string
		Flags         redisearch.Flag
		InKeys        []string
		ReturnFields  []string
		Language      string
		Expander      string
		Scorer        string
		SortBy        *redisearch.SortingKey
		HighlightOpts *redisearch.HighlightOptions
		SummarizeOpts *redisearch.SummaryOptions
	}
	tests := []struct {
		name   string
		fields fields
		want   redis.Args
	}{
		{"default", fields{Raw: ""}, redis.Args{"", "LIMIT", 0, 0}},
		{"Raw", fields{Raw: raw}, redis.Args{raw, "LIMIT", 0, 0}},
		{"QueryVerbatim", fields{Raw: raw, Flags: redisearch.QueryVerbatim}, redis.Args{raw, "LIMIT", 0, 0, "VERBATIM"}},
		{"QueryNoContent", fields{Raw: raw, Flags: redisearch.QueryNoContent}, redis.Args{raw, "LIMIT", 0, 0, "NOCONTENT"}},
		{"QueryInOrder", fields{Raw: raw, Flags: redisearch.QueryInOrder}, redis.Args{raw, "LIMIT", 0, 0, "INORDER"}},
		{"QueryWithPayloads", fields{Raw: raw, Flags: redisearch.QueryWithPayloads}, redis.Args{raw, "LIMIT", 0, 0, "WITHPAYLOADS"}},
		{"QueryWithScores", fields{Raw: raw, Flags: redisearch.QueryWithScores}, redis.Args{raw, "LIMIT", 0, 0, "WITHSCORES"}},
		{"InKeys", fields{Raw: raw, InKeys: []string{"test_key"}}, redis.Args{raw, "LIMIT", 0, 0, "INKEYS", 1, "test_key"}},
		{"ReturnFields", fields{Raw: raw, ReturnFields: []string{"test_field"}}, redis.Args{raw, "LIMIT", 0, 0, "RETURN", 1, "test_field"}},
		{"Language", fields{Raw: raw, Language: "chinese"}, redis.Args{raw, "LIMIT", 0, 0, "LANGUAGE", "chinese"}},
		{"Expander", fields{Raw: raw, Expander: "test_expander"}, redis.Args{raw, "LIMIT", 0, 0, "EXPANDER", "test_expander"}},
		{"Scorer", fields{Raw: raw, Scorer: "test_scorer"}, redis.Args{raw, "LIMIT", 0, 0, "SCORER", "test_scorer"}},
		{"SortBy", fields{Raw: raw, SortBy: &redisearch.SortingKey{
			Field:     "test_field",
			Ascending: true}}, redis.Args{raw, "LIMIT", 0, 0, "SORTBY", "test_field", "ASC"}},
		{"HighlightOpts", fields{Raw: raw, HighlightOpts: &redisearch.HighlightOptions{
			Fields: []string{"test_field"},
			Tags:   [2]string{"<tag>", "</tag>"},
		}}, redis.Args{raw, "LIMIT", 0, 0, "HIGHLIGHT", "FIELDS", 1, "test_field", "TAGS", "<tag>", "</tag>"}},
		{"SummarizeOpts", fields{Raw: raw, SummarizeOpts: &redisearch.SummaryOptions{
			Fields:       []string{"test_field"},
			FragmentLen:  20,
			NumFragments: 3,
			Separator:    "...",
		}}, redis.Args{raw, "LIMIT", 0, 0, "SUMMARIZE", "FIELDS", 1, "test_field", "LEN", 20, "FRAGS", 3, "SEPARATOR", "..."}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			q := redisearch.Query{
				Raw:           tt.fields.Raw,
				Flags:         tt.fields.Flags,
				InKeys:        tt.fields.InKeys,
				ReturnFields:  tt.fields.ReturnFields,
				Language:      tt.fields.Language,
				Expander:      tt.fields.Expander,
				Scorer:        tt.fields.Scorer,
				SortBy:        tt.fields.SortBy,
				HighlightOpts: tt.fields.HighlightOpts,
				SummarizeOpts: tt.fields.SummarizeOpts,
			}
			if g := q.Serialize(); !reflect.DeepEqual(g, tt.want) {
				t.Errorf("serialize() = %v, want %v", g, tt.want)
			}
		})
	}
}

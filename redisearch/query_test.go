package redisearch_test

import (
	"github.com/RediSearch/redisearch-go/redisearch"
	"github.com/garyburd/redigo/redis"
	"reflect"
	"testing"
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
		{"default with args", args{redisearch.DefaultIndexingOptions, redis.Args{"idx1", "doc1", 1.0}}, redis.Args{"idx1", "doc1", 1.0},},
		{"default", args{redisearch.DefaultIndexingOptions, redis.Args{}}, redis.Args{},},
		{"replace full doc", args{redisearch.IndexingOptions{Replace: true}, redis.Args{}}, redis.Args{"REPLACE"},},
		{"replace partial", args{redisearch.IndexingOptions{Replace: true, Partial: true}, redis.Args{}}, redis.Args{"REPLACE", "PARTIAL"},},
		{"replace if", args{redisearch.IndexingOptions{Replace: true, ReplaceCondition: "@timestamp < 23323234234"}, redis.Args{}}, redis.Args{"REPLACE", "IF", "@timestamp < 23323234234"},},
		{"replace partial if", args{redisearch.IndexingOptions{Replace: true, Partial: true, ReplaceCondition: "@timestamp < 23323234234"}, redis.Args{}}, redis.Args{"REPLACE", "PARTIAL", "IF", "@timestamp < 23323234234"},},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := redisearch.SerializeIndexingOptions(tt.args.opts, tt.args.args); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("serializeIndexingOptions() = %v, want %v", got, tt.want)
			}
		})
	}
}

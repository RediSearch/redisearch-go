package redisearch_test

import (
	"github.com/RediSearch/redisearch-go/redisearch"
	"github.com/garyburd/redigo/redis"
	"reflect"
	"testing"
)

func TestAutocompleter_Serialize(t *testing.T) {
	fuzzy := redisearch.DefaultSuggestOptions
	fuzzy.Fuzzy = true
	withscores := redisearch.DefaultSuggestOptions
	withscores.WithScores = true
	withpayloads := redisearch.DefaultSuggestOptions
	withpayloads.WithPayloads = true
	all := redisearch.DefaultSuggestOptions
	all.Fuzzy = true
	all.WithScores = true
	all.WithPayloads = true

	type fields struct {
		name string
	}
	type args struct {
		prefix string
		opts   redisearch.SuggestOptions
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   redis.Args
		want1  int
	}{
		{"default options", fields{"key1"}, args{"ab", redisearch.DefaultSuggestOptions,}, redis.Args{"key1", "ab", "MAX", 5}, 1},
		{"FUZZY", fields{"key1"}, args{"ab", fuzzy,}, redis.Args{"key1", "ab", "MAX", 5, "FUZZY"}, 1},
		{"WITHSCORES", fields{"key1"}, args{"ab", withscores,}, redis.Args{"key1", "ab", "MAX", 5, "WITHSCORES"}, 2},
		{"WITHPAYLOADS", fields{"key1"}, args{"ab", withpayloads,}, redis.Args{"key1", "ab", "MAX", 5, "WITHPAYLOADS"}, 2},
		{"all", fields{"key1"}, args{"ab", all,}, redis.Args{"key1", "ab", "MAX", 5, "FUZZY", "WITHSCORES", "WITHPAYLOADS"}, 3},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			a := redisearch.NewAutocompleter(tt.fields.name)
			got, got1 := a.Serialize(tt.args.prefix, tt.args.opts)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Serialize() got = %v, want %v", got, tt.want)
			}
			if got1 != tt.want1 {
				t.Errorf("Serialize() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}

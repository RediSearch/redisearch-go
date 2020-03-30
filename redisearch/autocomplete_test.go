package redisearch_test

import (
	"fmt"
	"github.com/RediSearch/redisearch-go/redisearch"
	"github.com/gomodule/redigo/redis"
	"github.com/stretchr/testify/assert"
	"os"
	"reflect"
	"testing"
)

func createAutocompleter(dictName string) *redisearch.Autocompleter {
	value, exists := os.LookupEnv("REDISEARCH_TEST_HOST")
	host := "localhost:6379"
	if exists && value != "" {
		host = value
	}
	return redisearch.NewAutocompleter(host, dictName)
}

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
			a := redisearch.NewAutocompleterFromPool(nil, tt.fields.name)
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

func TestSuggest(t *testing.T) {
	a := createAutocompleter("testing")

	// Add Terms to the Autocompleter
	terms := make([]redisearch.Suggestion, 10)
	for i := 0; i < 10; i++ {
		terms[i] = redisearch.Suggestion{Term: fmt.Sprintf("foo %d", i),
			Score: 1.0, Payload: fmt.Sprintf("bar %d", i)}
	}
	err := a.AddTerms(terms...)
	assert.Nil(t, err)
	suglen, err := a.Length()
	assert.Nil(t, err)
	assert.Equal(t, int64(10), suglen)
	// Retrieve Terms From Autocompleter - Without Payloads / Scores
	suggestions, err := a.SuggestOpts("f", redisearch.SuggestOptions{Num: 10})
	assert.Nil(t, err)
	assert.Equal(t, 10, len(suggestions))
	for _, suggestion := range suggestions {
		assert.Contains(t, suggestion.Term, "foo")
		assert.Equal(t, suggestion.Payload, "")
		assert.Zero(t, suggestion.Score)
	}

	// Retrieve Terms From Autocompleter - With Payloads & Scores
	suggestions, err = a.SuggestOpts("f", redisearch.SuggestOptions{Num: 10, WithScores: true, WithPayloads: true})
	assert.Nil(t, err)
	assert.Equal(t, 10, len(suggestions))
	for _, suggestion := range suggestions {
		assert.Contains(t, suggestion.Term, "foo")
		assert.Contains(t, suggestion.Payload, "bar")
		assert.NotZero(t, suggestion.Score)
	}
}

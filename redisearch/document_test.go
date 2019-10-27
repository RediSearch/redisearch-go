package redisearch_test

import (
	"github.com/RediSearch/redisearch-go/redisearch"
	"testing"
)

func TestEscapeTextFileString(t *testing.T) {
	type args struct {
		value string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			"url", args{"https://en.wikipedia.org/wiki",}, "https\\://en\\.wikipedia\\.org/wiki",
		},
		{
			"hello_world", args{"hello_world",}, "hello_world",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := redisearch.EscapeTextFileString(tt.args.value); got != tt.want {
				t.Errorf("EscapeTextFileString() = %v, want %v", got, tt.want)
			}
		})
	}
}

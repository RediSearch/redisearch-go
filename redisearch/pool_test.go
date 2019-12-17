package redisearch_test

import (
	"github.com/RediSearch/redisearch-go/redisearch"
	"os"
	"testing"
)

func TestNewMultiHostPool(t *testing.T) {
	value, exists := os.LookupEnv("REDISEARCH_TEST_HOST")
	host := "localhost:6379"
	if exists && value != "" {
		host = value
	}
	type args struct {
		hosts []string
	}
	tests := []struct {
		name string
		args args
	}{
		{"multihost same address", args{[]string{host,},},},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := redisearch.NewMultiHostPool(tt.args.hosts)
			conn := got.Get()
			if conn == nil {
				t.Errorf("NewMultiHostPool() = got nil connection")
			}
		})
	}
}

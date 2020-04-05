package redisearch

import (
	"testing"
)

func TestNewMultiHostPool(t *testing.T) {
	host, password := getTestConnectionDetails()
	type args struct {
		hosts []string
	}
	tests := []struct {
		name string
		args args
	}{
		{"multihost same address", args{[]string{host,},},},
	}
	if password == "" {
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				got := NewMultiHostPool(tt.args.hosts)
				conn := got.Get()
				if conn == nil {
					t.Errorf("NewMultiHostPool() = got nil connection")
				}
			})
		}
	}
}

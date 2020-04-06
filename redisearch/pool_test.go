package redisearch

import (
	"github.com/stretchr/testify/assert"
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
		{"multi-host single address", args{[]string{host,},},},
		{"multi-host several addresses", args{[]string{host, host,},},},
	}
	if password == "" {
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				got := NewMultiHostPool(tt.args.hosts)
				conn := got.Get()
				if conn == nil {
					t.Errorf("NewMultiHostPool() = got nil connection")
				}
				err := got.Close()
				assert.Nil(t, err)
			})
		}
	}
}

func TestMultiHostPool_Close(t *testing.T) {
	host, password := getTestConnectionDetails()
	if password == "" {
		oneMulti := NewMultiHostPool([]string{host,})
		conn := oneMulti.Get()
		assert.NotNil(t, conn)
		err := oneMulti.Close()
		assert.Nil(t, err)
		err = oneMulti.Close()
		assert.NotNil(t, conn)
		severalMulti := NewMultiHostPool([]string{host, host,})
		connMulti := severalMulti.Get()
		assert.NotNil(t, connMulti)
		err = severalMulti.Close()
		assert.Nil(t, err)
	}
}

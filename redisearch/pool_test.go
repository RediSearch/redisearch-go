package redisearch

import (
	"github.com/gomodule/redigo/redis"
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
	// Test a simple flow
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
	// Exhaustive test
	dial := func() (redis.Conn, error) {
		return redis.Dial("tcp", host, redis.DialPassword(password))
	}
	pool1 := &redis.Pool{Dial: dial, MaxIdle: maxConns}
	pool2 := &redis.Pool{Dial: dial, MaxIdle: maxConns}
	pool3 := &redis.Pool{Dial: dial, MaxIdle: maxConns}
	//Close pull3 prior to enforce error
	pool3.Close()
	pool4 := &redis.Pool{Dial: dial, MaxIdle: maxConns}

	type fields struct {
		pools map[string]*redis.Pool
		hosts []string
	}
	tests := []struct {
		name    string
		fields  fields
		wantErr bool
	}{
		{"empty", fields{map[string]*redis.Pool{}, []string{}}, false,},
		{"normal", fields{map[string]*redis.Pool{"hostpool1": pool1}, []string{"hostpool1"}}, false,},
		{"pool3-already-close", fields{map[string]*redis.Pool{"hostpool2": pool2, "hostpool3": pool3, "hostpool4": pool4,}, []string{"hostpool2", "hostpool3", "hostpool3"}}, false,},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &MultiHostPool{
				pools: tt.fields.pools,
				hosts: tt.fields.hosts,
			}
			if err := p.Close(); (err != nil) != tt.wantErr {
				t.Errorf("Close() error = %v, wantErr %v", err, tt.wantErr)
			}
			// ensure all connections are really closed
			if !tt.wantErr {
				for _, pool := range p.pools {
					if _, err := pool.Get().Do("PING"); err == nil {
						t.Errorf("expected error after connection closed. Got %v", err)
					}
				}
			}
		})
	}
}

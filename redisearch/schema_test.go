package redisearch

import (
	"reflect"
	"testing"
	"github.com/gomodule/redigo/redis"
)

func TestNewSchema(t *testing.T) {
	type args struct {
		opts Options
	}
	tests := []struct {
		name string
		args args
		want *Schema
	}{
		{"default", args{DefaultOptions}, &Schema{Fields: []Field{},
			Options: DefaultOptions}},
		{"custom-stopwords", args{Options{Stopwords: []string{"custom"}}}, &Schema{Fields: []Field{},
			Options: Options{Stopwords: []string{"custom"}}}},
		{"no-frequencies", args{Options{NoFrequencies: true}}, &Schema{Fields: []Field{},
			Options: Options{NoFrequencies: true}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := NewSchema(tt.args.opts); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewSchema() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSerializeSchema(t *testing.T) {
	type args struct {
		s    *Schema
		args redis.Args
	}
	tests := []struct {
		name    string
		args    args
		want    redis.Args
		wantErr bool
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := SerializeSchema(tt.args.s, tt.args.args)
			if (err != nil) != tt.wantErr {
				t.Errorf("SerializeSchema() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("SerializeSchema() got = %v, want %v", got, tt.want)
			}
		})
	}
}
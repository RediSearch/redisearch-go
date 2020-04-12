package redisearch

import (
	"github.com/gomodule/redigo/redis"
	"github.com/stretchr/testify/assert"
	"reflect"
	"testing"
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
		{"default-args", args{NewSchema(DefaultOptions), redis.Args{}}, redis.Args{"SCHEMA"}, false},
		{"no-frequencies", args{NewSchema(Options{NoFrequencies: true}), redis.Args{}}, redis.Args{"NOFREQS", "SCHEMA"}, false},
		{"no-offsets", args{NewSchema(Options{NoOffsetVectors: true}), redis.Args{}}, redis.Args{"NOOFFSETS", "SCHEMA"}, false},
		{"default-and-numeric", args{NewSchema(DefaultOptions).AddField(NewNumericField("numeric-field")), redis.Args{}}, redis.Args{"SCHEMA", "numeric-field", "NUMERIC"}, false},
		{"default-and-numeric-sortable", args{NewSchema(DefaultOptions).AddField(NewSortableNumericField("numeric-field")), redis.Args{}}, redis.Args{"SCHEMA", "numeric-field", "NUMERIC", "SORTABLE"}, false},
		{"default-and-numeric-with-options-noindex", args{NewSchema(DefaultOptions).AddField(NewNumericFieldOptions("numeric-field", NumericFieldOptions{NoIndex: true, Sortable: false})), redis.Args{}}, redis.Args{"SCHEMA", "numeric-field", "NUMERIC", "NOINDEX"}, false},
		{"default-and-text", args{NewSchema(DefaultOptions).AddField(NewTextField("text-field")), redis.Args{}}, redis.Args{"SCHEMA", "text-field", "TEXT"}, false},
		{"default-and-sortable-text-field", args{NewSchema(DefaultOptions).AddField(NewSortableTextField("text-field", 10)), redis.Args{}}, redis.Args{"SCHEMA", "text-field", "TEXT", "WEIGHT", float32(10.0), "SORTABLE"}, false},
		{"default-and-text-with-options", args{NewSchema(DefaultOptions).AddField(NewTextFieldOptions("text-field", TextFieldOptions{Weight: 5.0, Sortable: true, NoStem: false, NoIndex: false})), redis.Args{}}, redis.Args{"SCHEMA", "text-field", "TEXT", "WEIGHT", float32(5.0), "SORTABLE"}, false},
		{"default-and-tag", args{NewSchema(DefaultOptions).AddField(NewTagField("tag-field")), redis.Args{}}, redis.Args{"SCHEMA", "tag-field", "TAG", "SEPARATOR", ","}, false},
		{"default-and-tag-with-options", args{NewSchema(DefaultOptions).AddField(NewTagFieldOptions("tag-field", TagFieldOptions{Sortable: true, NoIndex: false, Separator: byte(',')})), redis.Args{}}, redis.Args{"SCHEMA", "tag-field", "TAG", "SEPARATOR", ",", "SORTABLE"}, false},
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
			assert.Equal(t, got, tt.want)
		})
	}
}

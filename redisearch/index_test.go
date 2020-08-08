package redisearch

import (
	"github.com/gomodule/redigo/redis"
	"reflect"
	"testing"
)

func TestIndexDefinition_Serialize(t *testing.T) {
	type fields struct {
		Definition *IndexDefinition
	}
	type args struct {
		args redis.Args
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   redis.Args
	}{
		{"default", fields{NewIndexDefinition()}, args{redis.Args{}}, redis.Args{"ON", "HASH"}},
		{"default+score", fields{NewIndexDefinition().SetScore(0.75)}, args{redis.Args{}}, redis.Args{"ON", "HASH", "SCORE", 0.75}},
		{"default+score_field", fields{NewIndexDefinition().SetScoreField("myscore")}, args{redis.Args{}}, redis.Args{"ON", "HASH", "SCORE_FIELD", "myscore"}},
		{"default+language", fields{NewIndexDefinition().SetLanguage("portuguese")}, args{redis.Args{}}, redis.Args{"ON", "HASH", "LANGUAGE", "portuguese"}},
		{"default+language_field", fields{NewIndexDefinition().SetLanguageField("mylanguage")}, args{redis.Args{}}, redis.Args{"ON", "HASH", "LANGUAGE_FIELD", "mylanguage"}},
		{"default+prefix", fields{NewIndexDefinition().AddPrefix("products:*")}, args{redis.Args{}}, redis.Args{"ON", "HASH", "PREFIX", 1, "products:*"}},
		{"default+payload_field", fields{NewIndexDefinition().SetPayloadField("products_description")}, args{redis.Args{}}, redis.Args{"ON", "HASH", "PAYLOAD_FIELD", "products_description"}},
		{"default+filter", fields{NewIndexDefinition().SetFilterExpression("@score:[0 50]")}, args{redis.Args{}}, redis.Args{"ON", "HASH", "FILTER", "@score:[0 50]"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			defintion := tt.fields.Definition
			if got := defintion.Serialize(tt.args.args); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Serialize() = %v, want %v", got, tt.want)
			}
		})
	}
}

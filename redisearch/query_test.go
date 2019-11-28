package redisearch

import (
	"github.com/garyburd/redigo/redis"
	"reflect"
	"testing"
)

func TestPaging_serialize(t *testing.T) {
	type fields struct {
		Offset int
		Num    int
	}
	tests := []struct {
		name   string
		fields fields
		want   redis.Args
	}{
		{"default", fields{0,10}, redis.Args{} },
		{"0-1000", fields{0,1000}, redis.Args{"LIMIT",0,1000} },
		{"100-200", fields{100,200}, redis.Args{"LIMIT",100,200} },
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := Paging{
				Offset: tt.fields.Offset,
				Num:    tt.fields.Num,
			}
			if got := p.serialize(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("serialize() = %v, want %v", got, tt.want)
			}
		})
	}
}
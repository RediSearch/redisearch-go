package redisearch

import (
	"github.com/gomodule/redigo/redis"
	"reflect"
	"testing"
)

func TestSpellCheckOptions_SetDistance(t *testing.T) {
	type fields struct {
		Distance       int
		ExclusionDicts []string
		InclusionDicts []string
	}
	type args struct {
		distance int
	}
	tests := []struct {
		name         string
		fields       fields
		args         args
		wantDistance int
		wantErr      bool
	}{
		{"error",
			fields{1, []string{}, []string{},},
			args{5},
			1,
			true,
		},
		{"4",
			fields{1, []string{}, []string{},},
			args{4},
			4,
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &SpellCheckOptions{
				Distance:       tt.fields.Distance,
				ExclusionDicts: tt.fields.ExclusionDicts,
				InclusionDicts: tt.fields.InclusionDicts,
			}
			got, err := s.SetDistance(tt.args.distance)
			if (err != nil) != tt.wantErr {
				t.Errorf("SetDistance() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got.Distance, tt.wantDistance) {
				t.Errorf("SetDistance() got = %v, want %v", got.Distance, tt.wantDistance)
			}
		})
	}
}

func TestSpellCheckOptions_AddExclusionDict(t *testing.T) {
	type args struct {
		dictname string
	}
	tests := []struct {
		name               string
		args               args
		wantExclusionDicts []string
	}{
		{"empty",
			args{"test"},
			[]string{"test"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := NewSpellCheckOptionsDefaults()
			if got := s.AddExclusionDict(tt.args.dictname); !reflect.DeepEqual(got.ExclusionDicts, tt.wantExclusionDicts) {
				t.Errorf("AddExclusionDict() = %v, want %v", got.ExclusionDicts, tt.wantExclusionDicts)
			}
		})
	}
}

func TestSpellCheckOptions_AddInclusionDict(t *testing.T) {
	type args struct {
		dictname string
	}
	tests := []struct {
		name               string
		args               args
		wantInclusionDicts []string
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := NewSpellCheckOptionsDefaults()
			if got := s.AddInclusionDict(tt.args.dictname); !reflect.DeepEqual(got.ExclusionDicts, tt.wantInclusionDicts) {
				t.Errorf("AddInclusionDict() = %v, want %v", got.ExclusionDicts, tt.wantInclusionDicts)
			}
		})
	}
}

func TestSpellCheckOptions_serialize(t *testing.T) {
	type fields struct {
		Distance       int
		ExclusionDicts []string
		InclusionDicts []string
	}
	tests := []struct {
		name   string
		fields fields
		want   redis.Args
	}{
		{"empty",
			fields{1, []string{}, []string{},},
			redis.Args{},
		},
		{"empty dicts distance 2",
			fields{2, []string{}, []string{},},
			redis.Args{"DISTANCE", 2},
		},
		{"both dicts distance 2",
			fields{2, []string{"excluded"}, []string{"included"},},
			redis.Args{"DISTANCE", 2, "TERMS", "EXCLUDE", "excluded", "TERMS", "INCLUDE", "included",},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := SpellCheckOptions{
				Distance:       tt.fields.Distance,
				ExclusionDicts: tt.fields.ExclusionDicts,
				InclusionDicts: tt.fields.InclusionDicts,
			}
			if got := s.serialize(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("serialize() = %v, want %v", got, tt.want)
			}
		})
	}
}

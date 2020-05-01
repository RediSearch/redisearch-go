package redisearch

import (
	"reflect"
	"testing"

	"github.com/gomodule/redigo/redis"
)

func TestMisspelledTerm_Len(t *testing.T) {
	type fields struct {
		Term                     string
		MisspelledSuggestionList []MisspelledSuggestion
	}
	tests := []struct {
		name   string
		fields fields
		want   int
	}{
		{"empty", fields{"empty", []MisspelledSuggestion{}}, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l := MisspelledTerm{
				Term:                     tt.fields.Term,
				MisspelledSuggestionList: tt.fields.MisspelledSuggestionList,
			}
			if got := l.Len(); got != tt.want {
				t.Errorf("Len() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestMisspelledTerm_Less(t *testing.T) {
	type fields struct {
		Term                     string
		MisspelledSuggestionList []MisspelledSuggestion
	}
	type args struct {
		i int
		j int
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   bool
	}{
		{"double-value-list-true", fields{"double", []MisspelledSuggestion{NewMisspelledSuggestion("double", 0), NewMisspelledSuggestion("doublee", 0.1)}}, args{1, 0}, true},
		{"double-value-list-false", fields{"double", []MisspelledSuggestion{NewMisspelledSuggestion("double", 0), NewMisspelledSuggestion("doublee", 0.1)}}, args{0, 1}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l := MisspelledTerm{
				Term:                     tt.fields.Term,
				MisspelledSuggestionList: tt.fields.MisspelledSuggestionList,
			}
			if got := l.Less(tt.args.i, tt.args.j); got != tt.want {
				t.Errorf("Less() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestMisspelledTerm_Sort(t *testing.T) {
	type fields struct {
		Term                     string
		MisspelledSuggestionList []MisspelledSuggestion
	}
	tests := []struct {
		name   string
		fields fields
		want   []MisspelledSuggestion
	}{
		{"empty", fields{"empty", []MisspelledSuggestion{}}, []MisspelledSuggestion{}},
		{"double-value-list", fields{"double", []MisspelledSuggestion{NewMisspelledSuggestion("double", 0), NewMisspelledSuggestion("doublee", 0.1)}}, []MisspelledSuggestion{NewMisspelledSuggestion("doublee", 0.1), NewMisspelledSuggestion("double", 0)}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l := MisspelledTerm{
				Term:                     tt.fields.Term,
				MisspelledSuggestionList: tt.fields.MisspelledSuggestionList,
			}
			l.Sort()
			if !reflect.DeepEqual(l.MisspelledSuggestionList, tt.want) {
				t.Errorf("Sort() = %v, want %v", l.MisspelledSuggestionList, tt.want)
			}
		})
	}
}

func TestMisspelledTerm_Swap(t *testing.T) {
	type fields struct {
		Term                     string
		MisspelledSuggestionList []MisspelledSuggestion
	}
	type args struct {
		i int
		j int
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   []MisspelledSuggestion
	}{
		{"empty-list", fields{"empty", []MisspelledSuggestion{}}, args{0, 1}, []MisspelledSuggestion{}},
		{"single-value-list", fields{"single", []MisspelledSuggestion{NewMisspelledSuggestion("first", 1)}}, args{0, 1}, []MisspelledSuggestion{NewMisspelledSuggestion("first", 1)}},
		{"double-value-list", fields{"doubl", []MisspelledSuggestion{NewMisspelledSuggestion("double", 1), NewMisspelledSuggestion("doublee", 0.1)}}, args{0, 1}, []MisspelledSuggestion{NewMisspelledSuggestion("doublee", 0.1), NewMisspelledSuggestion("double", 1)}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			l := MisspelledTerm{
				Term:                     tt.fields.Term,
				MisspelledSuggestionList: tt.fields.MisspelledSuggestionList,
			}
			l.Swap(tt.args.i, tt.args.j)
			if !reflect.DeepEqual(l.MisspelledSuggestionList, tt.want) {
				t.Errorf("Sort() = %v, want %v", l.MisspelledSuggestionList, tt.want)
			}
		})
	}
}

func TestNewMisspelledSuggestion(t *testing.T) {
	type args struct {
		term  string
		score float32
	}
	tests := []struct {
		name string
		args args
		want MisspelledSuggestion
	}{
		{"simple", args{"term", 1}, MisspelledSuggestion{"term", 1}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := NewMisspelledSuggestion(tt.args.term, tt.args.score); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewMisspelledSuggestion() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNewMisspelledTerm(t *testing.T) {
	type args struct {
		term string
	}
	tests := []struct {
		name string
		args args
		want MisspelledTerm
	}{
		{"1", args{"term"}, MisspelledTerm{"term", []MisspelledSuggestion{}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := NewMisspelledTerm(tt.args.term); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewMisspelledTerm() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestNewSpellCheckOptions(t *testing.T) {
	type args struct {
		distance int
	}
	tests := []struct {
		name          string
		args          args
		wantD         int
		wantExclusion []string
		wantInclusion []string
	}{
		{"1", args{1}, 1, []string{}, []string{}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := NewSpellCheckOptions(tt.args.distance)
			if !reflect.DeepEqual(got.Distance, tt.wantD) {
				t.Errorf("NewSpellCheckOptions() = %v, want %v", got.Distance, tt.wantD)
			}
			if !reflect.DeepEqual(got.ExclusionDicts, tt.wantExclusion) {
				t.Errorf("NewSpellCheckOptions() = %v, want %v", got.ExclusionDicts, tt.wantExclusion)
			}
			if !reflect.DeepEqual(got.InclusionDicts, tt.wantInclusion) {
				t.Errorf("NewSpellCheckOptions() = %v, want %v", got.InclusionDicts, tt.wantInclusion)
			}
		})
	}
}

func TestNewSpellCheckOptionsDefaults(t *testing.T) {
	tests := []struct {
		name          string
		wantD         int
		wantExclusion []string
		wantInclusion []string
	}{
		{"1", 1, []string{}, []string{}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := NewSpellCheckOptionsDefaults()
			if !reflect.DeepEqual(got.Distance, tt.wantD) {
				t.Errorf("TestNewSpellCheckOptionsDefaults() = %v, want %v", got.Distance, tt.wantD)
			}
			if !reflect.DeepEqual(got.ExclusionDicts, tt.wantExclusion) {
				t.Errorf("TestNewSpellCheckOptionsDefaults() = %v, want %v", got.ExclusionDicts, tt.wantExclusion)
			}
			if !reflect.DeepEqual(got.InclusionDicts, tt.wantInclusion) {
				t.Errorf("TestNewSpellCheckOptionsDefaults() = %v, want %v", got.InclusionDicts, tt.wantInclusion)
			}
		})
	}
}

func TestSpellCheckOptions_AddExclusionDict(t *testing.T) {
	type fields struct {
		Distance       int
		ExclusionDicts []string
		InclusionDicts []string
	}
	type args struct {
		dictname string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   []string
	}{
		{"empty", fields{1, []string{}, []string{}}, args{"dict1"}, []string{"dict1"}},
		{"one-prior", fields{1, []string{"dict1"}, []string{}}, args{"dict2"}, []string{"dict1", "dict2"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &SpellCheckOptions{
				Distance:       tt.fields.Distance,
				ExclusionDicts: tt.fields.ExclusionDicts,
				InclusionDicts: tt.fields.InclusionDicts,
			}
			if got := s.AddExclusionDict(tt.args.dictname).ExclusionDicts; !reflect.DeepEqual(got, tt.want) {
				t.Errorf("AddExclusionDict() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSpellCheckOptions_AddInclusionDict(t *testing.T) {
	type fields struct {
		Distance       int
		ExclusionDicts []string
		InclusionDicts []string
	}
	type args struct {
		dictname string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   []string
	}{
		{"empty", fields{1, []string{}, []string{}}, args{"dict1"}, []string{"dict1"}},
		{"one-prior", fields{1, []string{}, []string{"dict1"}}, args{"dict2"}, []string{"dict1", "dict2"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &SpellCheckOptions{
				Distance:       tt.fields.Distance,
				ExclusionDicts: tt.fields.ExclusionDicts,
				InclusionDicts: tt.fields.InclusionDicts,
			}
			if got := s.AddInclusionDict(tt.args.dictname).InclusionDicts; !reflect.DeepEqual(got, tt.want) {
				t.Errorf("AddInclusionDict() = %v, want %v", got, tt.want)
			}
		})
	}
}

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
		name    string
		fields  fields
		args    args
		want    int
		wantErr bool
	}{
		{"error-lower", fields{1, []string{}, []string{}}, args{0}, 1, true},
		{"error-upper", fields{1, []string{}, []string{}}, args{5}, 1, true},
		{"distance-4", fields{1, []string{}, []string{}}, args{4}, 4, false},
		{"distance-1", fields{4, []string{}, []string{}}, args{1}, 1, false},
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
			if !reflect.DeepEqual(got.Distance, tt.want) {
				t.Errorf("SetDistance() got = %v, want %v", got.Distance, tt.want)
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
		{"empty", fields{1, []string{}, []string{}}, redis.Args{}},
		{"exclude", fields{1, []string{"dict1"}, []string{}}, redis.Args{"TERMS", "EXCLUDE", "dict1"}},
		{"include", fields{1, []string{}, []string{"dict1"}}, redis.Args{"TERMS", "INCLUDE", "dict1"}},
		{"all", fields{2, []string{"dict1"}, []string{"dict2"}}, redis.Args{"DISTANCE", 2, "TERMS", "EXCLUDE", "dict1", "TERMS", "INCLUDE", "dict2"}},
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

func Test_loadMisspelledTerm(t *testing.T) {
	type args struct {
		arr     []interface{}
		termIdx int
		suggIdx int
	}
	// Each misspelled term, in turn, is a 3-element array consisting of
	// - the constant string "TERM" ( 3-element position 0 -- we dont use it )
	// - the term itself ( 3-element position 1 )
	// - an array of suggestions for spelling corrections ( 3-element position 2 )
	//termIdx := 1
	//suggIdx := 2
	//
	tests := []struct {
		name      string
		args      args
		wantMissT MisspelledTerm
		wantErr   bool
	}{
		{"empty", args{[]interface{}{}, 1, 2}, MisspelledTerm{}, false},
		{"missing term", args{[]interface{}{"TERM"}, 1, 2}, MisspelledTerm{}, true},
		{"missing sugestion array", args{[]interface{}{"TERM", "hockye"}, 1, 2}, MisspelledTerm{}, true},
		{"incorrect float", args{[]interface{}{"TERM", "hockye", []interface{}{[]interface{}{[]byte("INCORRECT"), []byte("hockey")}}}, 1, 2}, MisspelledTerm{}, true},
		{"correct1", args{[]interface{}{"TERM", "hockye", []interface{}{[]interface{}{[]byte("1"), []byte("hockey")}}}, 1, 2}, MisspelledTerm{"hockye", []MisspelledSuggestion{NewMisspelledSuggestion("hockey", 1.0)}}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotMissT, err := loadMisspelledTerm(tt.args.arr, tt.args.termIdx, tt.args.suggIdx)
			if (err != nil) != tt.wantErr {
				t.Errorf("loadMisspelledTerm() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(gotMissT, tt.wantMissT) {
				t.Errorf("loadMisspelledTerm() gotMissT = %v, want %v", gotMissT, tt.wantMissT)
			}
		})
	}
}

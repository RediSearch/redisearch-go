package redisearch

import (
	"reflect"
	"testing"
)

func TestEscapeTextFileString(t *testing.T) {
	type args struct {
		value string
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{
			"url", args{"https://en.wikipedia.org/wiki"}, "https\\://en\\.wikipedia\\.org/wiki",
		},
		{
			"hello_world", args{"hello_world"}, "hello_world",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := EscapeTextFileString(tt.args.value); got != tt.want {
				t.Errorf("EscapeTextFileString() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDocument_EstimateSize(t *testing.T) {
	type fields struct {
		Id         string
		Score      float32
		Payload    []byte
		Properties map[string]interface{}
	}
	tests := []struct {
		name   string
		fields fields
		wantSz int
	}{
		{
			"only-id", fields{"doc1", 1.0, []byte{}, map[string]interface{}{}}, len("doc1"),
		},
		{
			"id-payload", fields{"doc1", 1.0, []byte("payload"), map[string]interface{}{}}, len("doc1") + len([]byte("payload")),
		},
		{
			"id-payload-fields", fields{"doc1", 1.0, []byte("payload"), map[string]interface{}{"text1": []byte("text1")}}, len("doc1") + len([]byte("payload")) + 2*len([]byte("text1")),
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := &Document{
				Id:         tt.fields.Id,
				Score:      tt.fields.Score,
				Payload:    tt.fields.Payload,
				Properties: tt.fields.Properties,
			}
			if gotSz := d.EstimateSize(); !reflect.DeepEqual(gotSz, tt.wantSz) {
				t.Errorf("EstimateSize() = %v, want %v", gotSz, tt.wantSz)
			}
		})
	}
}

func TestDocument_SetPayload(t *testing.T) {
	type fields struct {
		Id         string
		Score      float32
		Payload    []byte
		Properties map[string]interface{}
	}
	type args struct {
		payload []byte
	}
	tests := []struct {
		name        string
		fields      fields
		args        args
		wantPayload []byte
	}{
		{"empty-payload", fields{"doc1", 1.0, []byte{}, map[string]interface{}{}}, args{[]byte{}}, []byte{}},
		{"simple-set", fields{"doc1", 1.0, []byte{}, map[string]interface{}{}}, args{[]byte("payload")}, []byte("payload")},
		{"set-with-previous-payload", fields{"doc1", 1.0, []byte("previous_payload"), map[string]interface{}{}}, args{[]byte("payload")}, []byte("payload")},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			d := &Document{
				Id:         tt.fields.Id,
				Score:      tt.fields.Score,
				Payload:    tt.fields.Payload,
				Properties: tt.fields.Properties,
			}
			d.SetPayload(tt.args.payload)
			if !reflect.DeepEqual(d.Payload, tt.wantPayload) {
				t.Errorf("SetPayload() = %v, want %v", d.Payload, tt.wantPayload)
			}
		})
	}
}

package redisearch

import (
	"github.com/gomodule/redigo/redis"
)

// IndexInfo - Structure showing information about an existing index
type IndexInfo struct {
	Schema               Schema
	Name                 string  `redis:"index_name"`
	DocCount             uint64  `redis:"num_docs"`
	RecordCount          uint64  `redis:"num_records"`
	TermCount            uint64  `redis:"num_terms"`
	MaxDocID             uint64  `redis:"max_doc_id"`
	InvertedIndexSizeMB  float64 `redis:"inverted_sz_mb"`
	OffsetVectorSizeMB   float64 `redis:"offset_vector_sz_mb"`
	DocTableSizeMB       float64 `redis:"doc_table_size_mb"`
	KeyTableSizeMB       float64 `redis:"key_table_size_mb"`
	RecordsPerDocAvg     float64 `redis:"records_per_doc_avg"`
	BytesPerRecordAvg    float64 `redis:"bytes_per_record_avg"`
	OffsetsPerTermAvg    float64 `redis:"offsets_per_term_avg"`
	OffsetBitsPerTermAvg float64 `redis:"offset_bits_per_record_avg"`
	IsIndexing           bool    `redis:"indexing"`
	PercentIndexed       float64 `redis:"percent_indexed"`
	HashIndexingFailures uint64  `redis:"hash_indexing_failures"`
}

// IndexDefinition is used to define a index definition for automatic indexing on Hash update
// This is only valid for >= RediSearch 2.0
type IndexDefinition struct {
	IndexOn          string
	Async            bool
	Prefix           []string
	FilterExpression string
	Language         string
	LanguageField    string
	Score            float64
	ScoreField       string
	PayloadField     string
}

// This is only valid for >= RediSearch 2.0
func NewIndexDefinition() *IndexDefinition {
	prefixArray := make([]string, 0)
	return &IndexDefinition{"HASH", false, prefixArray, "", "", "", -1, "", ""}
}

// This is only valid for >= RediSearch 2.0
func (defintion *IndexDefinition) SetAsync(value bool) (outDef *IndexDefinition) {
	outDef = defintion
	outDef.Async = value
	return
}

// This is only valid for >= RediSearch 2.0
func (defintion *IndexDefinition) AddPrefix(prefix string) (outDef *IndexDefinition) {
	outDef = defintion
	outDef.Prefix = append(outDef.Prefix, prefix)
	return
}

func (defintion *IndexDefinition) SetFilterExpression(value string) (outDef *IndexDefinition) {
	outDef = defintion
	outDef.FilterExpression = value
	return
}

// This is only valid for >= RediSearch 2.0
func (defintion *IndexDefinition) SetLanguage(value string) (outDef *IndexDefinition) {
	outDef = defintion
	outDef.Language = value
	return
}

// This is only valid for >= RediSearch 2.0
func (defintion *IndexDefinition) SetLanguageField(value string) (outDef *IndexDefinition) {
	outDef = defintion
	outDef.LanguageField = value
	return
}

// This is only valid for >= RediSearch 2.0
func (defintion *IndexDefinition) SetScore(value float64) (outDef *IndexDefinition) {
	outDef = defintion
	outDef.Score = value
	return
}

// This is only valid for >= RediSearch 2.0
func (defintion *IndexDefinition) SetScoreField(value string) (outDef *IndexDefinition) {
	outDef = defintion
	outDef.ScoreField = value
	return
}

// This is only valid for >= RediSearch 2.0
func (defintion *IndexDefinition) SetPayloadField(value string) (outDef *IndexDefinition) {
	outDef = defintion
	outDef.PayloadField = value
	return
}

// This is only valid for >= RediSearch 2.0
func (defintion *IndexDefinition) Serialize(args redis.Args) redis.Args {
	args = append(args, "ON", defintion.IndexOn)
	if defintion.Async {
		args = append(args, "ASYNC")
	}
	if len(defintion.Prefix) > 0 {
		args = append(args, "PREFIX", len(defintion.Prefix))
		for _, p := range defintion.Prefix {
			args = append(args, p)
		}
	}
	if defintion.FilterExpression != "" {
		args = append(args, "FILTER", defintion.FilterExpression)
	}
	if defintion.Language != "" {
		args = append(args, "LANGUAGE", defintion.Language)
	}

	if defintion.LanguageField != "" {
		args = append(args, "LANGUAGE_FIELD", defintion.LanguageField)
	}

	if defintion.Score >= 0.0 && defintion.Score <= 1.0 {
		args = append(args, "SCORE", defintion.Score)
	}

	if defintion.ScoreField != "" {
		args = append(args, "SCORE_FIELD", defintion.ScoreField)
	}
	if defintion.PayloadField != "" {
		args = append(args, "PAYLOAD_FIELD", defintion.PayloadField)
	}
	return args
}

func SerializeIndexingOptions(opts IndexingOptions, args redis.Args) redis.Args {
	// apply options

	// As of RediSearch 2.0 and above NOSAVE is no longer supported.
	if opts.NoSave {
		args = append(args, "NOSAVE")
	}
	if opts.Language != "" {
		args = append(args, "LANGUAGE", opts.Language)
	}

	if opts.Partial {
		opts.Replace = true
	}

	if opts.Replace {
		args = append(args, "REPLACE")
		if opts.Partial {
			args = append(args, "PARTIAL")
		}
		if opts.ReplaceCondition != "" {
			args = append(args, "IF", opts.ReplaceCondition)
		}
	}
	return args
}

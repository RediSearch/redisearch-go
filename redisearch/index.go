package redisearch

import "github.com/gomodule/redigo/redis"

func SerializeIndexingOptions(opts IndexingOptions, args redis.Args) redis.Args {
	// apply options
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
}

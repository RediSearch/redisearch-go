package redisearch

import (
	"errors"
	"fmt"
	"github.com/gomodule/redigo/redis"
)

// FieldType is an enumeration of field/property types
type FieldType int

// Options are flags passed to the the abstract Index call, which receives them as interface{}, allowing
// for implementation specific options
type Options struct {

	// If set, we will not save the documents contents, just index them, for fetching ids only.
	NoSave bool

	// If set, we avoid saving field bits for each term.
	// This saves memory, but does not allow filtering by specific fields.
	// This is an option that is applied and index level.
	NoFieldFlags bool

	// If set, we avoid saving the term frequencies in the index.
	// This saves memory but does not allow sorting based on the frequencies of a given term within the document.
	// This is an option that is applied and index level.
	NoFrequencies bool

	// If set, , we avoid saving the term offsets for documents.
	// This saves memory but does not allow exact searches or highlighting. Implies NOHL
	// This is an option that is applied and index level.
	NoOffsetVectors bool

	// Set the index with a custom stop-words list, to be ignored during indexing and search time
	// This is an option that is applied and index level.
	// If the list is nil the default stop-words list is used.
	// See https://oss.redislabs.com/redisearch/Stopwords.html#default_stop-word_list
	Stopwords []string
}

// DefaultOptions represents the default options
var DefaultOptions = Options{
	NoSave:          false,
	NoFieldFlags:    false,
	NoFrequencies:   false,
	NoOffsetVectors: false,
	Stopwords:       nil,
}

const (
	// TextField full-text field
	TextField FieldType = iota

	// NumericField numeric range field
	NumericField

	// GeoField geo-indexed point field
	GeoField

	// TagField is a field used for compact indexing of comma separated values
	TagField
)

// Field represents a single field's Schema
type Field struct {
	Name     string
	Type     FieldType
	Sortable bool
	Options  interface{}
}

// TextFieldOptions Options for text fields - weight and stemming enabled/disabled.
type TextFieldOptions struct {
	Weight   float32
	Sortable bool
	NoStem   bool
	NoIndex  bool
}

// TagFieldOptions options for indexing tag fields
type TagFieldOptions struct {
	// Separator is the custom separator between tags. defaults to comma (,)
	Separator byte
	NoIndex   bool
	Sortable  bool
}

// NumericFieldOptions Options for numeric fields
type NumericFieldOptions struct {
	Sortable bool
	NoIndex  bool
}

// NewTextField creates a new text field with the given weight
func NewTextField(name string) Field {
	return Field{
		Name:    name,
		Type:    TextField,
		Options: nil,
	}
}

// NewTextFieldOptions creates a new text field with given options (weight/sortable)
func NewTextFieldOptions(name string, opts TextFieldOptions) Field {
	f := NewTextField(name)
	f.Options = opts
	return f
}

// NewSortableTextField creates a text field with the sortable flag set
func NewSortableTextField(name string, weight float32) Field {
	return NewTextFieldOptions(name, TextFieldOptions{
		Weight:   weight,
		Sortable: true,
	})

}

// NewTagField creates a new text field with default options (separator: ,)
func NewTagField(name string) Field {
	return Field{
		Name:    name,
		Type:    TagField,
		Options: TagFieldOptions{Separator: ',', NoIndex: false},
	}
}

// NewTagFieldOptions creates a new tag field with the given options
func NewTagFieldOptions(name string, opts TagFieldOptions) Field {
	return Field{
		Name:    name,
		Type:    TagField,
		Options: opts,
	}
}

// NewNumericField creates a new numeric field with the given name
func NewNumericField(name string) Field {
	return Field{
		Name: name,
		Type: NumericField,
	}
}

// NewNumericFieldOptions defines a numeric field with additional options
func NewNumericFieldOptions(name string, options NumericFieldOptions) Field {
	f := NewNumericField(name)
	f.Options = options
	return f
}

// NewSortableNumericField creates a new numeric field with the given name and a sortable flag
func NewSortableNumericField(name string) Field {
	f := NewNumericField(name)
	f.Options = NumericFieldOptions{
		Sortable: true,
	}
	return f
}

// Schema represents an index schema Schema, or how the index would
// treat documents sent to it.
type Schema struct {
	Fields  []Field
	Options Options
}

// NewSchema creates a new Schema object
func NewSchema(opts Options) *Schema {
	return &Schema{
		Fields: []Field{},
		Options: opts,
	}
}

// AddField adds a field to the Schema object
func (m *Schema) AddField(f Field) *Schema {
	if m.Fields == nil {
		m.Fields = []Field{}
	}
	m.Fields = append(m.Fields, f)
	return m
}


func SerializeSchema(s *Schema, args redis.Args) (redis.Args, error) {
	if s.Options.NoFieldFlags {
		args = append(args, "NOFIELDS")
	}
	if s.Options.NoFrequencies {
		args = append(args, "NOFREQS")
	}
	if s.Options.NoOffsetVectors {
		args = append(args, "NOOFFSETS")
	}
	if s.Options.Stopwords != nil {
		args = args.Add("STOPWORDS", len(s.Options.Stopwords))
		if len(s.Options.Stopwords) > 0 {
			args = args.AddFlat(s.Options.Stopwords)
		}
	}

	args = append(args, "SCHEMA")
	for _, f := range s.Fields {

		switch f.Type {
		case TextField:

			args = append(args, f.Name, "TEXT")
			if f.Options != nil {
				opts, ok := f.Options.(TextFieldOptions)
				if !ok {
					return nil, errors.New("Invalid text field options type")
				}

				if opts.Weight != 0 && opts.Weight != 1 {
					args = append(args, "WEIGHT", opts.Weight)
				}
				if opts.NoStem {
					args = append(args, "NOSTEM")
				}

				if opts.Sortable {
					args = append(args, "SORTABLE")
				}

				if opts.NoIndex {
					args = append(args, "NOINDEX")
				}
			}

		case NumericField:
			args = append(args, f.Name, "NUMERIC")
			if f.Options != nil {
				opts, ok := f.Options.(NumericFieldOptions)
				if !ok {
					return nil, errors.New("Invalid numeric field options type")
				}

				if opts.Sortable {
					args = append(args, "SORTABLE")
				}
				if opts.NoIndex {
					args = append(args, "NOINDEX")
				}
			}
		case TagField:
			args = append(args, f.Name, "TAG")
			if f.Options != nil {
				opts, ok := f.Options.(TagFieldOptions)
				if !ok {
					return nil, errors.New("Invalid tag field options type")
				}
				if opts.Separator != 0 {
					args = append(args, "SEPARATOR", fmt.Sprintf("%c", opts.Separator))

				}
				if opts.Sortable {
					args = append(args, "SORTABLE")
				}
				if opts.NoIndex {
					args = append(args, "NOINDEX")
				}
			}
		default:
			return nil, fmt.Errorf("Unsupported field type %v", f.Type)
		}

	}
	return args, nil
}
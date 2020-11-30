package redisearch

import (
	"fmt"
	"github.com/gomodule/redigo/redis"
)

// FieldType is an enumeration of field/property types
type FieldType int

// PhoneticMatcherType is an enumeration of the phonetic algorithm and language used.
type PhoneticMatcherType string

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

	// If set to true, creates a lightweight temporary index which will expire after the specified period of inactivity.
	// The internal idle timer is reset whenever the index is searched or added to.
	// Because such indexes are lightweight, you can create thousands of such indexes without negative performance implications.
	Temporary       bool
	TemporaryPeriod int

	// For efficiency, RediSearch encodes indexes differently if they are created with less than 32 text fields.
	// If set to true This option forces RediSearch to encode indexes as if there were more than 32 text fields,
	// which allows you to add additional fields (beyond 32).
	MaxTextFieldsFlag bool
}

func NewOptions() *Options {
	var opts = DefaultOptions
	return &opts
}

// If set to true, creates a lightweight temporary index which will expire after the specified period of inactivity.
// The internal idle timer is reset whenever the index is searched or added to.
// To enable the temporary index creation, use SetTemporaryPeriod(). This method should be preferably used for disabling the flag
func (options *Options) SetTemporary(temporary bool) *Options {
	options.Temporary = temporary
	return options
}

// If set to a positive integer, creates a lightweight temporary index which will expire after the specified period of inactivity (in seconds).
// The internal idle timer is reset whenever the index is searched or added to.
func (options *Options) SetTemporaryPeriod(period int) *Options {
	options.TemporaryPeriod = period
	options.Temporary = true
	return options
}

// Set the index with a custom stop-words list, to be ignored during indexing and search time
// This is an option that is applied and index level.
// If the list is nil the default stop-words list is used.
// See https://oss.redislabs.com/redisearch/Stopwords.html#default_stop-word_list
func (options *Options) SetStopWords(stopwords []string) *Options {
	options.Stopwords = stopwords
	return options
}

// For efficiency, RediSearch encodes indexes differently if they are created with less than 32 text fields.
// If set to true, this flag forces RediSearch to encode indexes as if there were more than 32 text fields,
// which allows you to add additional fields (beyond 32).
func (options *Options) SetMaxTextFieldsFlag(flag bool) *Options {
	options.MaxTextFieldsFlag = flag
	return options
}

// DefaultOptions represents the default options
var DefaultOptions = Options{
	NoSave:            false,
	NoFieldFlags:      false,
	NoFrequencies:     false,
	NoOffsetVectors:   false,
	Stopwords:         nil,
	Temporary:         false,
	TemporaryPeriod:   0,
	MaxTextFieldsFlag: false,
}

// Field Types
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

// Phonetic Matchers
const (
	PhoneticDoubleMetaphoneEnglish    PhoneticMatcherType = "dm:en"
	PhoneticDoubleMetaphoneFrench     PhoneticMatcherType = "dm:fr"
	PhoneticDoubleMetaphonePortuguese PhoneticMatcherType = "dm:pt"
	PhoneticDoubleMetaphoneSpanish    PhoneticMatcherType = "dm:es"
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
	Weight          float32
	Sortable        bool
	NoStem          bool
	NoIndex         bool
	PhoneticMatcher PhoneticMatcherType
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

// GeoFieldOptions Options for geo fields
type GeoFieldOptions struct {
	NoIndex bool
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

// NewGeoField creates a new geo field with the given name
func NewGeoField(name string) Field {
	return Field{
		Name:    name,
		Type:    GeoField,
		Options: nil,
	}
}

// NewGeoFieldOptions creates a new geo field with the given name and additional options
func NewGeoFieldOptions(name string, options GeoFieldOptions) Field {
	f := NewGeoField(name)
	f.Options = options
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
		Fields:  []Field{},
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

func SerializeSchema(s *Schema, args redis.Args) (argsOut redis.Args, err error) {
	argsOut = args
	if s.Options.MaxTextFieldsFlag {
		argsOut = append(argsOut, "MAXTEXTFIELDS")
	}
	if s.Options.NoOffsetVectors {
		argsOut = append(argsOut, "NOOFFSETS")
	}
	if s.Options.Temporary {
		argsOut = append(argsOut, "TEMPORARY", s.Options.TemporaryPeriod)
	}
	if s.Options.NoFieldFlags {
		argsOut = append(argsOut, "NOFIELDS")
	}
	if s.Options.NoFrequencies {
		argsOut = append(argsOut, "NOFREQS")
	}

	if s.Options.Stopwords != nil {
		argsOut = argsOut.Add("STOPWORDS", len(s.Options.Stopwords))
		if len(s.Options.Stopwords) > 0 {
			argsOut = argsOut.AddFlat(s.Options.Stopwords)
		}
	}

	argsOut = append(argsOut, "SCHEMA")
	for _, f := range s.Fields {
		argsOut, err = serializeField(f, argsOut)
		if err != nil {
			return nil, err
		}
	}
	return
}

func serializeField(f Field, args redis.Args) (argsOut redis.Args, err error) {
	argsOut = args
	switch f.Type {
	case TextField:
		argsOut = append(argsOut, f.Name, "TEXT")
		if f.Options != nil {
			opts, ok := f.Options.(TextFieldOptions)
			if !ok {
				err = fmt.Errorf("Error on TextField serialization")
				return
			}
			if opts.Weight != 0 && opts.Weight != 1 {
				argsOut = append(argsOut, "WEIGHT", opts.Weight)
			}
			if opts.NoStem {
				argsOut = append(argsOut, "NOSTEM")
			}
			if opts.PhoneticMatcher != "" {
				argsOut = append(argsOut, "PHONETIC", string(opts.PhoneticMatcher))
			}
			if opts.Sortable {
				argsOut = append(argsOut, "SORTABLE")
			}
			if opts.NoIndex {
				argsOut = append(argsOut, "NOINDEX")
			}
		}
	case NumericField:
		argsOut = append(argsOut, f.Name, "NUMERIC")
		if f.Options != nil {
			opts, ok := f.Options.(NumericFieldOptions)
			if !ok {
				err = fmt.Errorf("Error on NumericField serialization")
				return
			}
			if opts.Sortable {
				argsOut = append(argsOut, "SORTABLE")
			}
			if opts.NoIndex {
				argsOut = append(argsOut, "NOINDEX")
			}
		}
	case TagField:
		argsOut = append(argsOut, f.Name, "TAG")
		if f.Options != nil {
			opts, ok := f.Options.(TagFieldOptions)
			if !ok {
				err = fmt.Errorf("Error on TagField serialization")
				return
			}
			if opts.Separator != 0 {
				argsOut = append(argsOut, "SEPARATOR", fmt.Sprintf("%c", opts.Separator))
			}
			if opts.Sortable {
				argsOut = append(argsOut, "SORTABLE")
			}
			if opts.NoIndex {
				argsOut = append(argsOut, "NOINDEX")
			}
		}
	case GeoField:
		argsOut = append(argsOut, f.Name, "GEO")
		if f.Options != nil {
			opts, ok := f.Options.(GeoFieldOptions)
			if !ok {
				err = fmt.Errorf("Error on GeoField serialization")
				return
			}
			if opts.NoIndex {
				argsOut = append(argsOut, "NOINDEX")
			}
		}
	default:
		err = fmt.Errorf("Unrecognized field type %v serialization", f.Type)
		return
	}
	return
}

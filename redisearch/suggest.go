package redisearch

import "sort"

// Suggestion is a single suggestion being added or received from the Autocompleter
type Suggestion struct {
	Term    string
	Score   float64
	Payload string
	Incr    bool
}

// SuggestOptions are options which are passed when recieving suggestions from the Autocompleter
type SuggestOptions struct {
	Num          int
	Fuzzy        bool
	WithPayloads bool
	WithScores   bool
}

// DefaultIndexingOptions are the default options for document indexing
var DefaultSuggestOptions = SuggestOptions{
	Num:          5,
	Fuzzy:        false,
	WithPayloads: false,
	WithScores:   false,
}

// SuggestionList is a sortable list of suggestions returned from an engine
type SuggestionList []Suggestion

func (l SuggestionList) Len() int           { return len(l) }
func (l SuggestionList) Swap(i, j int)      { l[i], l[j] = l[j], l[i] }
func (l SuggestionList) Less(i, j int) bool { return l[i].Score > l[j].Score } //reverse sorting

// Sort the SuggestionList
func (l SuggestionList) Sort() {
	sort.Sort(l)
}

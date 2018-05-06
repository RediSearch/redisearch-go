package redisearch

import "sort"

// Suggestion is a single suggestion being added or received from the Autocompleter
type Suggestion struct {
	Term    string
	Score   float64
	Payload string
}

// SuggestOptions are options which are passed when recieving suggestions from the Autocompleter
type SuggestOptions struct {
	Num          int
	Fuzzy        bool
	WithPayloads bool
	WithScores   bool
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

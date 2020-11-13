package redisearch

import (
	"fmt"
	"github.com/gomodule/redigo/redis"
	"sort"
)

// SpellCheckOptions are options which are passed when performing spelling correction on a query
type SpellCheckOptions struct {
	Distance       int
	ExclusionDicts []string
	InclusionDicts []string
}

func NewSpellCheckOptionsDefaults() *SpellCheckOptions {
	return &SpellCheckOptions{
		Distance:       1,
		ExclusionDicts: make([]string, 0),
		InclusionDicts: make([]string, 0),
	}
}

func NewSpellCheckOptions(distance int) *SpellCheckOptions {
	return &SpellCheckOptions{
		Distance:       distance,
		ExclusionDicts: make([]string, 0),
		InclusionDicts: make([]string, 0),
	}
}

// SetDistance Sets the the maximal Levenshtein distance for spelling suggestions (default: 1, max: 4)
func (s *SpellCheckOptions) SetDistance(distance int) (*SpellCheckOptions, error) {
	if distance < 1 || distance > 4 {
		return s, fmt.Errorf("The maximal Levenshtein distance for spelling suggestions should be between [1,4]. Got %d", distance)
	} else {
		s.Distance = distance
	}
	return s, nil
}

// AddExclusionDict adds a custom dictionary named {dictname} to the exclusion list
func (s *SpellCheckOptions) AddExclusionDict(dictname string) *SpellCheckOptions {
	s.ExclusionDicts = append(s.ExclusionDicts, dictname)
	return s
}

// AddInclusionDict adds a custom dictionary named {dictname} to the inclusion list
func (s *SpellCheckOptions) AddInclusionDict(dictname string) *SpellCheckOptions {
	s.InclusionDicts = append(s.InclusionDicts, dictname)
	return s
}

func (s SpellCheckOptions) serialize() redis.Args {
	args := redis.Args{}
	if s.Distance > 1 {
		args = args.Add("DISTANCE").Add(s.Distance)
	}
	for _, exclusion := range s.ExclusionDicts {
		args = args.Add("TERMS").Add("EXCLUDE").Add(exclusion)
	}
	for _, inclusion := range s.InclusionDicts {
		args = args.Add("TERMS").Add("INCLUDE").Add(inclusion)
	}
	return args
}

// MisspelledSuggestion is a single suggestion from the spelling corrections
type MisspelledSuggestion struct {
	Suggestion string
	Score      float32
}

// NewMisspelledSuggestion creates a MisspelledSuggestion with the specific term and score
func NewMisspelledSuggestion(term string, score float32) MisspelledSuggestion {
	return MisspelledSuggestion{
		Suggestion: term,
		Score:      score,
	}
}

// MisspelledTerm contains the misspelled term and a sortable list of suggestions returned from an engine
type MisspelledTerm struct {
	Term string
	// MisspelledSuggestionList is a sortable list of suggestions returned from an engine
	MisspelledSuggestionList []MisspelledSuggestion
}

func NewMisspelledTerm(term string) MisspelledTerm {
	return MisspelledTerm{
		Term:                     term,
		MisspelledSuggestionList: make([]MisspelledSuggestion, 0),
	}
}

func (l MisspelledTerm) Len() int { return len(l.MisspelledSuggestionList) }
func (l MisspelledTerm) Swap(i, j int) {
	maxLen := len(l.MisspelledSuggestionList)
	if i < maxLen && j < maxLen {
		l.MisspelledSuggestionList[i], l.MisspelledSuggestionList[j] = l.MisspelledSuggestionList[j], l.MisspelledSuggestionList[i]
	}
}
func (l MisspelledTerm) Less(i, j int) bool {
	return l.MisspelledSuggestionList[i].Score > l.MisspelledSuggestionList[j].Score
} //reverse sorting

// Sort the SuggestionList
func (l MisspelledTerm) Sort() {
	sort.Sort(l)
}

// convert the result from a redis spelling correction on a query to a proper MisspelledTerm object
func loadMisspelledTerm(arr []interface{}, termIdx, suggIdx int) (missT MisspelledTerm, err error) {
	if len(arr) == 0 {
		return MisspelledTerm{}, nil
	}
	if termIdx >= len(arr) {
		return MisspelledTerm{}, fmt.Errorf("term index: (%d) is larger than reply size: %d", termIdx, len(arr))
	}
	term, err := redis.String(arr[termIdx], err)
	if err != nil {
		return MisspelledTerm{}, fmt.Errorf("Could not parse term: %s", err)
	}
	missT = NewMisspelledTerm(term)
	if suggIdx >= len(arr) {
		return MisspelledTerm{}, fmt.Errorf("suggestion index: (%d) is larger than reply size: %d", suggIdx, len(arr))
	}
	lst, err := redis.Values(arr[suggIdx], err)
	if err != nil {
		return MisspelledTerm{}, fmt.Errorf("Could not get the array of suggestions for spelling corrections on term %s. Error: %s", term, err)
	}
	for i := 0; i < len(lst); i++ {
		innerLst, err := redis.Values(lst[i], err)
		if err != nil {
			return MisspelledTerm{}, fmt.Errorf("Could not get the inner array of suggestions for spelling corrections on term %s. Error: %s", term, err)
		}
		if len(innerLst) != 2 {
			return MisspelledTerm{}, fmt.Errorf("expects 2 elements per inner-array")
		}
		score, err := redis.Float64(innerLst[0], err)
		if err != nil {
			return MisspelledTerm{}, fmt.Errorf("Could not parse score: %s", err)
		}
		suggestion, err := redis.String(innerLst[1], err)
		if err != nil {
			return MisspelledTerm{}, fmt.Errorf("Could not parse suggestion: %s", err)
		}
		missT.MisspelledSuggestionList = append(missT.MisspelledSuggestionList, NewMisspelledSuggestion(suggestion, float32(score)))
	}

	return missT, nil
}

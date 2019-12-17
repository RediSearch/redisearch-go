package redisearch

import (
	"strconv"

	"github.com/garyburd/redigo/redis"
)

// Autocompleter implements a redisearch auto-completer API
type Autocompleter struct {
	name string
}

// NewAutocompleter creates a new Autocompleter with the given host and key name
func NewAutocompleter(name string) *Autocompleter {
	return &Autocompleter{
		name: name,
	}
}

func (a *Autocompleter) Serialize(prefix string, opts SuggestOptions) (redis.Args, int) {
	inc := 1
	args := redis.Args{a.name, prefix, "MAX", opts.Num}
	if opts.Fuzzy {
		args = append(args, "FUZZY")
	}
	if opts.WithScores {
		args = append(args, "WITHSCORES")
		inc++
	}
	if opts.WithPayloads {
		args = append(args, "WITHPAYLOADS")
		inc++
	}
	return args, inc
}

func ProcessSugGetVals(vals []string, inc int, WithScores, WithPayloads bool) (ret []Suggestion) {
	ret = make([]Suggestion, 0, len(vals)/inc)
	for i := 0; i < len(vals); i += inc {

		suggestion := Suggestion{Term: vals[i]}
		if WithScores {
			score, err := strconv.ParseFloat(vals[i+1], 64)
			if err != nil {
				continue
			}
			suggestion.Score = score
		}
		if WithPayloads {
			suggestion.Payload = vals[i+(inc-1)]
		}
		ret = append(ret, suggestion)

	}
	return
}

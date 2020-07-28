package redisearch

import (
	"github.com/gomodule/redigo/redis"
	"strconv"
)

// Autocompleter implements a redisearch auto-completer API
type Autocompleter struct {
	name string
	pool *redis.Pool
}

// NewAutocompleter creates a new Autocompleter with the given pool and key name
func NewAutocompleterFromPool(pool *redis.Pool, name string) *Autocompleter {
	return &Autocompleter{name: name, pool: pool}
}

// NewAutocompleter creates a new Autocompleter with the given host and key name
func NewAutocompleter(addr, name string) *Autocompleter {
	return &Autocompleter{
		pool: redis.NewPool(func() (redis.Conn, error) {
			return redis.Dial("tcp", addr)
		}, maxConns),
		name: name,
	}
}

// Delete deletes the Autocompleter key for this AC
func (a *Autocompleter) Delete() error {
	conn := a.pool.Get()
	defer conn.Close()

	_, err := conn.Do("DEL", a.name)
	return err
}

// AddTerms pushes new term suggestions to the index
func (a *Autocompleter) AddTerms(terms ...Suggestion) error {
	conn := a.pool.Get()
	defer conn.Close()

	i := 0
	for _, term := range terms {

		args := redis.Args{a.name, term.Term, term.Score}
		if term.Incr {
			args = append(args, "INCR")
		}
		if term.Payload != "" {
			args = append(args, "PAYLOAD", term.Payload)
		}

		if err := conn.Send("FT.SUGADD", args...); err != nil {
			return err
		}
		i++
	}
	if err := conn.Flush(); err != nil {
		return err
	}
	for i > 0 {
		if _, err := conn.Receive(); err != nil {
			return err
		}
		i--
	}
	return nil
}

// AddTerms pushes new term suggestions to the index
func (a *Autocompleter) DeleteTerms(terms ...Suggestion) error {
	conn := a.pool.Get()
	defer conn.Close()

	i := 0
	for _, term := range terms {

		args := redis.Args{a.name, term.Term}
		if err := conn.Send("FT.SUGDEL", args...); err != nil {
			return err
		}
		i++
	}
	if err := conn.Flush(); err != nil {
		return err
	}
	for i > 0 {
		if _, err := conn.Receive(); err != nil {
			return err
		}
		i--
	}
	return nil
}

// AddTerms pushes new term suggestions to the index
func (a *Autocompleter) Length() (len int64, err error) {
	conn := a.pool.Get()
	defer conn.Close()
	len, err = redis.Int64(conn.Do("FT.SUGLEN", a.name))
	return
}

// Suggest gets completion suggestions from the Autocompleter dictionary to the given prefix.
// If fuzzy is set, we also complete for prefixes that are in 1 Levenshten distance from the
// given prefix
//
// Deprecated: Please use SuggestOpts() instead
func (a *Autocompleter) Suggest(prefix string, num int, fuzzy bool) (ret []Suggestion, err error) {
	conn := a.pool.Get()
	defer conn.Close()

	seropts := DefaultSuggestOptions
	seropts.Num = num
	seropts.Fuzzy = fuzzy
	args, inc := a.Serialize(prefix, seropts)

	vals, err := redis.Strings(conn.Do("FT.SUGGET", args...))
	if err != nil {
		return nil, err
	}

	ret = ProcessSugGetVals(vals, inc, true, false)

	return
}

// SuggestOpts gets completion suggestions from the Autocompleter dictionary to the given prefix.
// SuggestOptions are passed allowing you specify if the returned values contain a payload, and scores.
// If SuggestOptions.Fuzzy is set, we also complete for prefixes that are in 1 Levenshtein distance from the
// given prefix
func (a *Autocompleter) SuggestOpts(prefix string, opts SuggestOptions) (ret []Suggestion, err error) {
	conn := a.pool.Get()
	defer conn.Close()

	args, inc := a.Serialize(prefix, opts)
	vals, err := redis.Strings(conn.Do("FT.SUGGET", args...))
	if err != nil {
		return nil, err
	}

	ret = ProcessSugGetVals(vals, inc, opts.WithScores, opts.WithPayloads)

	return
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

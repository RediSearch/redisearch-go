package redisearch

import (
	"strconv"

	"github.com/garyburd/redigo/redis"
)

// Autocompleter implements a redisearch auto-completer API
type Autocompleter struct {
	pool *redis.Pool
	name string
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
		if payload := term.Payload; payload != "" {
			args = append(args, "PAYLOAD", payload)
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

// Suggest gets completion suggestions from the Autocompleter dictionary to the given prefix.
// If fuzzy is set, we also complete for prefixes that are in 1 Levenshten distance from the
// given prefix
func (a *Autocompleter) Suggest(prefix string, num int, fuzzy, withPayloads bool) ([]Suggestion, error) {
	conn := a.pool.Get()
	defer conn.Close()

	inc := 2

	args := redis.Args{a.name, prefix, "MAX", num, "WITHSCORES"}
	if fuzzy {
		args = append(args, "FUZZY")
	}
	if withPayloads {
		args = append(args, "WITHPAYLOADS")
		inc = 3
	}
	vals, err := redis.Strings(conn.Do("FT.SUGGET", args...))
	if err != nil {
		return nil, err
	}

	ret := make([]Suggestion, 0, len(vals)/inc)
	for i := 0; i < len(vals); i += inc {

		score, err := strconv.ParseFloat(vals[i+1], 64)
		if err != nil {
			continue
		}

		suggestion := Suggestion{Term: vals[i], Score: score}
		if withPayloads {
			suggestion.Payload = vals[i+2]
		}
		ret = append(ret, suggestion)

	}

	return ret, nil

}

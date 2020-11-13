package redisearch

import (
	"log"

	redigo "github.com/gomodule/redigo/redis"
)

type ConnectionPool struct {
	RedigoPool   *redigo.Pool
	RedigoClient redigo.Conn
	IndexName    string

	RedisearchIndexList map[string]string
}

// Search searches the index for the given query, and returns documents,
// the total number of results, or an error if something went wrong
func (i *ConnectionPool) ConfigPoolSearch(q *Query) (docs []Document, total int, err error) {
	conn := i.RedigoClient
	defer conn.Close()

	args := redigo.Args{i.IndexName}
	args = append(args, q.serialize()...)

	res, err := redigo.Values(conn.Do("FT.SEARCH", args...))
	if err != nil {
		return
	}

	if total, err = redigo.Int(res[0], nil); err != nil {
		return
	}

	docs = make([]Document, 0, len(res)-1)

	skip := 1
	scoreIdx := -1
	fieldsIdx := -1
	payloadIdx := -1
	if q.Flags&QueryWithScores != 0 {
		scoreIdx = 1
		skip++
	}
	if q.Flags&QueryWithPayloads != 0 {
		payloadIdx = skip
		skip++
	}

	if q.Flags&QueryNoContent == 0 {
		fieldsIdx = skip
		skip++
	}

	if len(res) > skip {
		for i := 1; i < len(res); i += skip {

			if d, e := loadDocument(res, i, scoreIdx, payloadIdx, fieldsIdx); e == nil {
				docs = append(docs, d)
			} else {
				log.Print("Error parsing doc: ", e)
			}
		}
	}
	return
}

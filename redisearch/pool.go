package redisearch

import (
	"math/rand"
	"sync"
	"time"

	"github.com/garyburd/redigo/redis"
)

type ConnPool interface {
	Get() redis.Conn
}

type SingleHostPool struct {
	*redis.Pool
}

func NewSingleHostPool(host string) *SingleHostPool {
	ret := redis.NewPool(func() (redis.Conn, error) {
		// TODO: Add timeouts. and 2 separate pools for indexing and querying, with different timeouts
		return redis.Dial("tcp", host)
	}, maxConns)
	ret.TestOnBorrow = func(c redis.Conn, t time.Time) (err error) {
		if time.Since(t) > time.Second {
			_, err = c.Do("PING")
		}
		return err
	}
	return &SingleHostPool{ret}
}

type MultiHostPool struct {
	sync.Mutex
	pools map[string]*redis.Pool
	hosts []string
}

func NewMultiHostPool(hosts []string) *MultiHostPool {

	return &MultiHostPool{
		pools: make(map[string]*redis.Pool, len(hosts)),
		hosts: hosts,
	}
}

func (p *MultiHostPool) Get() redis.Conn {
	p.Lock()
	defer p.Unlock()
	host := p.hosts[rand.Intn(len(p.hosts))]
	pool, found := p.pools[host]
	if !found {
		pool = redis.NewPool(func() (redis.Conn, error) {
			// TODO: Add timeouts. and 2 separate pools for indexing and querying, with different timeouts
			return redis.Dial("tcp", host)
		}, maxConns)
		pool.TestOnBorrow = func(c redis.Conn, t time.Time) error {
			if time.Since(t).Seconds() > 1 {
				_, err := c.Do("PING")
				return err
			}
			return nil
		}

		p.pools[host] = pool
	}
	return pool.Get()

}

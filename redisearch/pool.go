package redisearch

import (
	"github.com/gomodule/redigo/redis"
	"math/rand"
	"sync"
	"time"
)

type ConnPool interface {
	Get() redis.Conn
	Close() error
}

type SingleHostPool struct {
	*redis.Pool
}

func NewSingleHostPool(host string) *SingleHostPool {
	ret := &redis.Pool{Dial: func() (redis.Conn, error) {
		return redis.Dial("tcp", host)
	}, MaxIdle: maxConns}
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
		pool.TestOnBorrow = func(c redis.Conn, t time.Time) (err error) {
			if time.Since(t) > time.Second {
				_, err = c.Do("PING")
			}
			return err
		}

		p.pools[host] = pool
	}
	return pool.Get()

}

func (p *MultiHostPool) Close() (err error) {
	p.Lock()
	defer p.Unlock()
	for _, pool := range p.pools {
		poolErr := pool.Close()
		//preserve pool error if not nil but continue
		if poolErr != nil {
			err = poolErr
		}
	}
	return
}

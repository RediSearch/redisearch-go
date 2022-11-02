package redisearch

import (
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/gomodule/redigo/redis"
)

type ConnPool interface {
	Get() redis.Conn
	Close() error
}

type SingleHostPool struct {
	*redis.Pool
}

func NewSingleHostPool(host string, options ...redis.DialOption) *SingleHostPool {
	pool := &redis.Pool{Dial: func() (redis.Conn, error) {
		return redis.Dial("tcp", host, options...)
	}, MaxIdle: maxConns}
	pool.TestOnBorrow = func(c redis.Conn, t time.Time) (err error) {
		if time.Since(t) > time.Second {
			_, err = c.Do("PING")
		}
		return err
	}
	return &SingleHostPool{pool}
}

type MultiHostPool struct {
	sync.Mutex
	pools   map[string]*redis.Pool
	hosts   []string
	options []redis.DialOption
}

func NewMultiHostPool(hosts []string, options ...redis.DialOption) *MultiHostPool {

	return &MultiHostPool{
		pools:   make(map[string]*redis.Pool, len(hosts)),
		hosts:   hosts,
		options: options,
	}
}

func (p *MultiHostPool) Get() redis.Conn {
	p.Lock()
	defer p.Unlock()
	host := p.hosts[rand.Intn(len(p.hosts))]
	pool, found := p.pools[host]
	if !found {
		pool = &redis.Pool{
			Dial: func() (redis.Conn, error) {
				return redis.Dial("tcp", host, p.options...)
			},
			TestOnBorrow: func(c redis.Conn, t time.Time) error {
				if time.Since(t) < time.Minute {
					return nil
				}
				_, err := c.Do("PING")
				return err
			},
		}

		p.pools[host] = pool
	}
	return pool.Get()

}

func (p *MultiHostPool) Close() (err error) {
	p.Lock()
	defer p.Unlock()
	for host, pool := range p.pools {
		poolErr := pool.Close()
		//preserve pool error if not nil but continue
		if poolErr != nil {
			if err == nil {
				err = fmt.Errorf("Error closing pool for host %s. Got %v.", host, poolErr)
			} else {
				err = fmt.Errorf("%v Error closing pool for host %s. Got %v.", err, host, poolErr)
			}
		}
	}
	return
}

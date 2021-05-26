package redis

import (
	"github.com/crawlab-team/crawlab-db/interfaces"
	"time"
)

type Option func(c interfaces.RedisClient)

func WithBackoffMaxInterval(interval time.Duration) Option {
	return func(c interfaces.RedisClient) {
		c.SetBackoffMaxInterval(interval)
	}
}

func WithTimeout(timeout int) Option {
	return func(c interfaces.RedisClient) {
		c.SetTimeout(timeout)
	}
}

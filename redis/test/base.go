package test

import (
	"github.com/crawlab-team/crawlab-db/interfaces"
	"github.com/crawlab-team/crawlab-db/redis"
	"testing"
)

func init() {
	var err error
	T, err = NewTest()
	if err != nil {
		panic(err)
	}
}

type Test struct {
	client          interfaces.RedisClient
	TestCollection  string
	TestMessage     string
	TestMessages    []string
	TestMessagesMap map[string]string
	TestLockKey     string
}

func (t *Test) Setup(t2 *testing.T) {
	t2.Cleanup(t.Cleanup)
}

func (t *Test) Cleanup() {
	_ = t.client.Del(t.TestCollection)
}

var T *Test

func NewTest() (t *Test, err error) {
	// test
	t = &Test{}

	// client
	t.client, err = redis.GetRedisClient()
	if err != nil {
		return nil, err
	}

	// test collection
	t.TestCollection = "test_collection"

	// test message
	t.TestMessage = "this is a test message"

	// test messages
	t.TestMessages = []string{
		"test message 1",
		"test message 2",
		"test message 3",
	}

	// test messages map
	t.TestMessagesMap = map[string]string{
		"test key 1": "test value 1",
		"test key 2": "test value 2",
		"test key 3": "test value 3",
	}

	// test lock key
	t.TestLockKey = "test lock key"

	return t, nil
}

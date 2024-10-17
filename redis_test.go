package golang_redis

import (
	"context"
	"fmt"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"strconv"
	"testing"
	"time"
)

var client = redis.NewClient(&redis.Options{
	Addr: "localhost:6379",
	DB:   0,
})

func TestConnection(t *testing.T) {
	assert.NotNil(t, client)
}

var ctx = context.Background()

func TestPing(t *testing.T) {
	result, err := client.Ping(ctx).Result()
	if err != nil {
		fmt.Println("Error while pinging Redis:", err)
	} else {
		fmt.Println("Ping result:", result)
	}

	assert.Nil(t, err)
	assert.Equal(t, "PONG", result)
}

func TestString(t *testing.T) {
	client.SetEx(ctx, "name", "Brian", 3*time.Second)

	result, err := client.Get(ctx, "name").Result()
	assert.Nil(t, err)
	assert.Equal(t, "Brian", result)

	time.Sleep(5 * time.Second)

	result, err = client.Get(ctx, "name").Result()
	assert.NotNil(t, err)
}

func TestList(t *testing.T) {
	client.RPush(ctx, "names", "Brian")
	client.RPush(ctx, "names", "Anashari")

	assert.Equal(t, "Brian", client.LPop(ctx, "names").Val())
	assert.Equal(t, "Anashari", client.LPop(ctx, "names").Val())
}

func TestSet(t *testing.T) {
	client.SAdd(ctx, "students", "Brian")
	client.SAdd(ctx, "students", "Brian")
	client.SAdd(ctx, "students", "Anashari")
	client.SAdd(ctx, "students", "Anashari")

	assert.Equal(t, int64(2), client.SCard(ctx, "students").Val())
	assert.Equal(t, []string{"Brian", "Anashari"}, client.SMembers(ctx, "students").
		Val())
}

func TestSortedSet(t *testing.T) {
	client.ZAdd(ctx, "scores", redis.Z{Score: 100, Member: "Brian"})
	client.ZAdd(ctx, "scores", redis.Z{Score: 85, Member: "Anashari"})

	assert.Equal(t, []string{"Anashari", "Brian"}, client.ZRange(ctx, "scores", 0, -1).Val())

	assert.Equal(t, "Brian", client.ZPopMax(ctx, "scores").Val()[0].Member)
	assert.Equal(t, "Anashari", client.ZPopMax(ctx, "scores").Val()[0].Member)
}

func TestHash(t *testing.T) {
	client.HSet(ctx, "user:1", "id", "1")
	client.HSet(ctx, "user:1", "name", "Brian")
	client.HSet(ctx, "user:1", "email", "brian@example.com")

	user := client.HGetAll(ctx, "user:1").Val()
	assert.Equal(t, "Brian", user["name"])
	assert.Equal(t, "brian@example.com", user["email"])
	assert.Equal(t, "1", user["id"])

	client.Del(ctx, "user:1")
}

func TestGeoPoint(t *testing.T) {
	client.GeoAdd(ctx, "activities", &redis.GeoLocation{
		Name:      "TULT",
		Latitude:  -6.96924884875154,
		Longitude: 107.62821114517274,
	})

	client.GeoAdd(ctx, "activities", &redis.GeoLocation{
		Name:      "KOS",
		Latitude:  -6.977847483359247,
		Longitude: 107.6333577248925,
	})

	assert.Equal(t, 1.1126, client.GeoDist(ctx, "activities", "TULT", "KOS", "km").Val())

	search := client.GeoSearch(ctx, "activities", &redis.GeoSearchQuery{
		Latitude:   -6.974320501525047,
		Longitude:  107.63108915376415,
		Radius:     5,
		RadiusUnit: "km",
	}).Val()

	assert.Equal(t, []string{"TULT", "KOS"}, search)
}

func TestHyperLogLog(t *testing.T) {
	client.PFAdd(ctx, "visitors", "brian", "anashari", "sari")
	client.PFAdd(ctx, "visitors", "sari", "puyol", "anashari")
	client.PFAdd(ctx, "visitors", "celox", "dusk", "puyol")

	count := client.PFCount(ctx, "visitors").Val()
	assert.Equal(t, int64(6), count)
}

func TestPipeline(t *testing.T) {
	_, err := client.Pipelined(ctx, func(pipeliner redis.Pipeliner) error {
		pipeliner.SetEx(ctx, "first_name", "Brian", 3*time.Second)
		pipeliner.SetEx(ctx, "last_name", "Anashari", 3*time.Second)

		return nil
	})

	assert.Nil(t, err)

	assert.Equal(t, "Brian", client.Get(ctx, "first_name").Val())
	assert.Equal(t, "Anashari", client.Get(ctx, "last_name").Val())
}

func TestTransaction(t *testing.T) {
	_, err := client.TxPipelined(ctx, func(pipeliner redis.Pipeliner) error {
		pipeliner.SetEx(ctx, "first_name", "Brian", 3*time.Second)
		pipeliner.SetEx(ctx, "last_name", "Anashari", 3*time.Second)

		return nil
	})

	assert.Nil(t, err)

	assert.Equal(t, "Brian", client.Get(ctx, "first_name").Val())
	assert.Equal(t, "Anashari", client.Get(ctx, "last_name").Val())
}

func TestPublishStream(t *testing.T) {
	for i := 0; i < 10; i++ {
		err := client.XAdd(ctx, &redis.XAddArgs{
			Stream: "members",
			Values: map[string]interface{}{
				"name":    "Eko",
				"address": "Indonesia",
			},
		}).Err()
		assert.Nil(t, err)
	}
}

func TestCreateConsumerGroup(t *testing.T) {
	client.XGroupCreate(ctx, "members", "group-1", "0")
	client.XGroupCreateConsumer(ctx, "members", "group-1", "consumer-1")
	client.XGroupCreateConsumer(ctx, "members", "group-1", "consumer-2")
}

func TestConsumeStream(t *testing.T) {
	streams := client.XReadGroup(ctx, &redis.XReadGroupArgs{
		Group:    "group-1",
		Consumer: "consumer-1",
		Streams:  []string{"members", ">"},
		Count:    2,
		Block:    5 * time.Second,
	}).Val()

	for _, stream := range streams {
		for _, message := range stream.Messages {
			fmt.Println(message.ID)
			fmt.Println(message.Values)
		}
	}
}

func TestSubscribePubSub(t *testing.T) {
	subscriber := client.Subscribe(ctx, "channel-1")
	defer subscriber.Close()
	for i := 0; i < 10; i++ {
		message, err := subscriber.ReceiveMessage(ctx)
		assert.Nil(t, err)
		fmt.Println(message.Payload)
	}
}

func TestPublishPubSub(t *testing.T) {
	for i := 0; i < 10; i++ {
		err := client.Publish(ctx, "channel-1", "Hello "+strconv.Itoa(i)).Err()
		assert.Nil(t, err)
	}
}

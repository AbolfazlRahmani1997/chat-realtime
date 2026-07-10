package RedisRepository

import (
	"fmt"
	"github.com/go-redis/redis"
)

type RedisRepository struct {
	Client *redis.Client
	Prefix string
}

func New(Prefix string) *RedisRepository {
	client := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       0,
	})

	return &RedisRepository{
		Client: client,
		Prefix: Prefix,
	}
}

func (Redis *RedisRepository) Insert(Key string, Value interface{}) (interface{}, error) {
	err := Redis.Client.Set(Key, Value, 0).Err()
	if err != nil {
		return nil, err
	}
	return nil, err
}

func (Redis *RedisRepository) Get(Key string) (interface{}, error) {
	result := Redis.Client.Get(Key)
	if result != nil {
		fmt.Print(result)
	}
	return result, nil

}

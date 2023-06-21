package service

import (
	"bee-copy/pkg/config"
	"bee-copy/pkg/model/status"
	"context"
	"errors"
	"github.com/redis/go-redis/v9"
	"time"
)

type RedisService struct {
	client *redis.Client
	ctx    context.Context
}

func NewRedisService(rc config.RedisConfig) *RedisService {
	rdb := redis.NewFailoverClient(&redis.FailoverOptions{
		MasterName:    rc.MasterName,
		SentinelAddrs: rc.Host,
		Password:      rc.Password,
		DB:            rc.Database,
		MaxRetries:    5,
	})

	return &RedisService{rdb, context.Background()}
}

func (r *RedisService) Save(key string, value string) {
	_ = r.client.RPush(r.ctx, key, value).Err()
	r.SearchExpire(key)
}

func (r *RedisService) Time(key string, dt int64) {
	_ = r.client.Set(r.ctx, "go-time of "+key, dt, time.Hour*3).Err()
}

func (r *RedisService) SearchExpire(key string) {
	_ = r.client.Expire(r.ctx, key, time.Minute*10).Err()
}

func (r *RedisService) GetStateOfCopyEvent(key string) (string, error) {
	res, err := r.client.HMGet(r.ctx, key, "status").Result()
	if err != nil {
		return "", err
	}

	if len(res) == 0 || res[0] == nil {
		return "", errors.New("not found record on Redis")
	}

	return res[0].(string), nil
}

func (r *RedisService) UpdateStateCopyEvent(key, state string) error {
	return r.client.HMSet(r.ctx, key, map[string]interface{}{"status": state}).Err()
}

func (r *RedisService) ErrorStateCopyEvent(key, msg string) error {
	return r.client.HMSet(r.ctx, key, map[string]interface{}{"error": msg, "status": status.ERROR}).Err()
}

func (r *RedisService) AddLogError(value string) {
	_ = r.client.RPush(r.ctx, "copy-error", value).Err()
}

func (r *RedisService) Increase() {
	_ = r.client.Incr(r.ctx, "copy_count").Err()
}

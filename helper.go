package redis4rag

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
)

func CreateIndex(cli *redis.Client, schema []*redis.FieldSchema, index string, prefix []interface{}) (err error) {
	cmd := cli.FTCreate(context.Background(), index,
		&redis.FTCreateOptions{Prefix: prefix, OnJSON: true},
		schema...)
	_, err = cmd.Result()
	if err != nil {
		return err
	}
	for {
		var res redis.FTInfoResult
		res, err = cli.FTInfo(context.Background(), index).Result()
		if err != nil {
			return err
		}
		if res.Indexing == 0 {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
}

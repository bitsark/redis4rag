package redis4rag

import (
	"cmp"
	"context"
	"os"
	"testing"

	"github.com/redis/go-redis/v9"
	should "github.com/stretchr/testify/assert"
)

func TestLLMCacheBasic(t *testing.T) {
	ctx := context.Background()
	indexname := "idx:test_llm_cache_basic"
	docprefix := "doc:test_llm_cache_basic"
	redisCli := redis.NewClient(&redis.Options{
		Addr:     cmp.Or(os.Getenv("REDIS_URL"), "localhost:6379"),
		Protocol: 2,
	})
	redisCli.FTDropIndexWithArgs(ctx, indexname, &redis.FTDropIndexOptions{DeleteDocs: true})

	err := CreateIndex(redisCli, LLMCacheSchema, indexname, []interface{}{docprefix})
	should.Nil(t, err)
	t.Logf("index %s created", indexname)

	//TODO
	cache := &LLMsCache{
		indexName: indexname,
		docPrefix: docprefix,
		redisCli:  redisCli,
	}

	err = cache.Cache(ctx, &QueryAnswer{
		Tag:    "chatter",
		Query:  "咱俩谁跟谁呀。",
		Answer: `“咱俩谁跟谁呀”是一句比较口语化的表达，意思是说“你我之间关系很亲近、很熟悉，不需要客气”或者“你我之间不存在什么隔阂、矛盾”。这句话通常用于比较熟络的人之间，表达一种亲近感和随意感，也可以用来缓和气氛，表示“咱们之间不用那么见外”。`,
	}, localEmbedder.Embedding)
	should.Nil(t, err)

	err = cache.Cache(ctx, &QueryAnswer{
		Tag:    "chatter",
		Query:  "我俩谁跟谁呀。",
		Answer: `“我俩谁跟谁呀”这句话的意思和“咱俩谁跟谁呀”类似，但语气上可能会稍有不同，具体含义需要结合语境来判断`,
	}, localEmbedder.Embedding)

	should.Nil(t, err)

	{
		qa, err := cache.Lookup(ctx, "咱俩谁跟谁呀。")
		should.Nil(t, err)
		should.NotNil(t, qa)
		t.Log(qa)
	}

	{
		qa, err := cache.SemanticSearch(ctx, "chatter", "咱俩关系不错呀。", localEmbedder.Embedding)
		should.Nil(t, err)
		should.NotNil(t, qa)
		t.Log(qa)
	}

	err = redisCli.FTDropIndexWithArgs(context.Background(), indexname, &redis.FTDropIndexOptions{DeleteDocs: true}).Err()
	should.Nil(t, err)
	t.Logf("index %s dropped", indexname)
}

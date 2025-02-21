package redis4rag

import (
	"cmp"
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	should "github.com/stretchr/testify/assert"
)

func TestChatHistoryBasic(t *testing.T) {
	indexname := "idx:test_chat_history_basic"
	docprefix := "doc:test_chat_history_basic"
	redisCli := redis.NewClient(&redis.Options{
		Addr:     cmp.Or(os.Getenv("REDIS_URL"), "localhost:6379"),
		Protocol: 2,
	})
	redisCli.FTDropIndexWithArgs(context.Background(), indexname, &redis.FTDropIndexOptions{DeleteDocs: true})

	err := CreateIndex(redisCli, ChatHistorySchema, indexname, []interface{}{docprefix})
	should.Nil(t, err)
	t.Logf("index %s created", indexname)

	//TODO
	chatHistory := &ChatHistory{
		indexName: indexname,
		docPrefix: docprefix,
		redisCli:  redisCli,
	}

	t1 := time.Now()
	time.Sleep(1 * time.Millisecond)
	err = chatHistory.Add(context.Background(), &ChatMessage{
		Typ:       "user",
		UserId:    "user_id",
		SessionId: "session_id",
		Content:   "咱俩谁跟谁呀。",
		Timestamp: time.Now().UnixMilli(),
	}, localEmbedder.Embedding)
	should.Nil(t, err)

	t2 := time.Now()
	time.Sleep(1 * time.Millisecond)

	err = chatHistory.Add(context.Background(), &ChatMessage{
		Typ:       "user",
		UserId:    "user_id",
		SessionId: "session_id_2",
		Content:   "我俩谁跟谁呀。",
		Timestamp: time.Now().UnixMilli(),
	}, localEmbedder.Embedding)
	should.Nil(t, err)

	t3 := time.Now()
	// list
	{
		msgs, err := chatHistory.ListBySessionId(context.Background(), t1.UnixMilli(), "session_id")
		should.Nil(t, err)
		buf, err := json.Marshal(msgs)
		should.Nil(t, err)
		t.Logf("%s\n", string(buf))
	}
	{
		msgs, err := chatHistory.ListBySessionId(context.Background(), t2.UnixMilli(), "session_id")
		should.Nil(t, err)
		buf, err := json.Marshal(msgs)
		should.Nil(t, err)
		t.Logf("%s\n", string(buf))
	}
	{
		msgs, err := chatHistory.ListBySessionId(context.Background(), t3.UnixMilli(), "session_id")
		should.Nil(t, err)
		buf, err := json.Marshal(msgs)
		should.Nil(t, err)
		t.Logf("%s\n", string(buf))
	}

	//search
	{
		msgs, err := chatHistory.SearchWithUserId(context.Background(),
			t1.UnixMilli(), "user_id", "咱俩关系不错呀。", localEmbedder.Embedding)
		should.Nil(t, err)
		buf, err := json.Marshal(msgs)
		should.Nil(t, err)
		t.Logf("%s\n", string(buf))
	}

	{
		err := chatHistory.DeleteBySessionId(context.Background(), "session_id")
		should.Nil(t, err)
		msgs, err := chatHistory.SearchWithUserId(context.Background(),
			t1.UnixMilli(), "user_id", "咱俩关系不错呀。", localEmbedder.Embedding)
		should.Nil(t, err)
		buf, err := json.Marshal(msgs)
		should.Nil(t, err)
		t.Logf("%s\n", string(buf))

	}

	err = redisCli.FTDropIndexWithArgs(context.Background(), indexname, &redis.FTDropIndexOptions{DeleteDocs: true}).Err()
	should.Nil(t, err)
	t.Logf("index %s dropped", indexname)
}

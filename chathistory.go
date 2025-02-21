package redis4rag

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/redis/go-redis/v9"
)

var (
	ChatHistorySchema []*redis.FieldSchema = []*redis.FieldSchema{
		ChatMessageUserId,
		ChatMessageSessionId,
		ChatMessageType,
		ChatMessageContent,
		ChatMessageTimestamp,
		ChatMessageContentVec,
	}

	ChatMessageDefaultReturn = []redis.FTSearchReturn{
		{FieldName: ChatMessageUserId.FieldName},
		{FieldName: ChatMessageSessionId.FieldName},
		{FieldName: ChatMessageType.FieldName},
		{FieldName: ChatMessageContent.FieldName},
		{FieldName: ChatMessageTimestamp.FieldName},
	}

	ChatMessageUserId = &redis.FieldSchema{
		FieldName: "$.user_id", As: "user_id", FieldType: redis.SearchFieldTypeText, NoStem: true,
	}
	ChatMessageSessionId = &redis.FieldSchema{
		FieldName: "$.session_id", As: "session_id", FieldType: redis.SearchFieldTypeText,
	}
	ChatMessageType = &redis.FieldSchema{
		FieldName: "$.type", As: "type", FieldType: redis.SearchFieldTypeTag, Separator: ",",
	}
	ChatMessageContent = &redis.FieldSchema{
		FieldName: "$.content", As: "content", FieldType: redis.SearchFieldTypeText,
	}
	ChatMessageTimestamp = &redis.FieldSchema{
		FieldName: "$.timestamp", As: "timestamp", FieldType: redis.SearchFieldTypeNumeric, Sortable: true,
	}
	ChatMessageContentVec = &redis.FieldSchema{
		FieldName: "$.content_vec", As: "content_vec", FieldType: redis.SearchFieldTypeVector,
		VectorArgs: &redis.FTVectorArgs{
			FlatOptions: &redis.FTFlatOptions{
				Type:           "FLOAT64",
				Dim:            1024,
				DistanceMetric: "COSINE",
			},
		},
	}
)

type (
	ChatHistory struct {
		indexName string
		docPrefix string
		redisCli  *redis.Client
	}

	ChatMessage struct {
		Typ       string `json:"type"`
		UserId    string `json:"user_id"`
		SessionId string `json:"session_id"`
		Content   string `json:"content"`
		Timestamp int64  `json:"timestamp"`
	}
)

func (history *ChatHistory) Add(ctx context.Context, msg *ChatMessage, embedder Embedder) (err error) {
	var vec []float64
	if vec, err = embedder(ctx, msg.Content); err != nil {
		return
	}
	var jsondata []byte
	if jsondata, err = json.Marshal(msg); err != nil {
		return
	}
	pipeline := history.redisCli.Pipeline()
	// key pattern: {ChatHistory.DocPrefix}:{Message.UserId}:{Message.SessionId}
	key := fmt.Sprintf("%s:%s:%s:%d", history.docPrefix, msg.UserId, msg.SessionId, msg.Timestamp)
	pipeline.JSONSet(ctx, key, "$", string(jsondata))
	pipeline.JSONSet(ctx, key, ChatMessageContentVec.FieldName, vec)
	_, err = pipeline.Exec(ctx)
	return err
}

func (history *ChatHistory) ListByUserId(ctx context.Context, from int64, userId string) (msgs []*ChatMessage, err error) {
	return history.list(ctx, from, ChatMessageUserId.As, userId)
}

func (history *ChatHistory) ListBySessionId(ctx context.Context, from int64, sessionId string) (msgs []*ChatMessage, err error) {
	return history.list(ctx, from, ChatMessageSessionId.As, sessionId)
}

func (history *ChatHistory) SearchWithUserId(ctx context.Context, from int64, userId string, text string, embedder Embedder) (msgs []*ChatMessage, err error) {
	return history.search(ctx, from, ChatMessageUserId.As, userId, text, embedder)
}

func (history *ChatHistory) SearchWithSessionId(ctx context.Context, from int64, sessionId string, text string, embedder Embedder) (msgs []*ChatMessage, err error) {
	return history.search(ctx, from, ChatMessageUserId.As, sessionId, text, embedder)
}

func (history *ChatHistory) DeleteByUserId(ctx context.Context, userId string) error {
	return history.delete(ctx, ChatMessageUserId.As, userId)
}

func (history *ChatHistory) DeleteBySessionId(ctx context.Context, sessionId string) error {
	return history.delete(ctx, ChatMessageSessionId.As, sessionId)
}

func (history *ChatHistory) list(ctx context.Context, from int64, key, val string) (msgs []*ChatMessage, err error) {
	opts := &redis.FTSearchOptions{
		Return:         ChatMessageDefaultReturn,
		DialectVersion: 2,
	}
	query := fmt.Sprintf("@%s:%s @%s:[%d inf]", key, val, ChatMessageTimestamp.As, from)
	cmd := history.redisCli.FTSearchWithArgs(ctx, history.indexName, query, opts)
	var result redis.FTSearchResult
	if result, err = cmd.Result(); err == nil && result.Total > 0 {
		for _, doc := range result.Docs {
			msg := parseChatMessage(&doc)
			msgs = append(msgs, msg)
		}
	}
	return
}

func (history *ChatHistory) search(ctx context.Context, from int64, key, val string, text string, embedder Embedder) (msgs []*ChatMessage, err error) {
	var vec []float64
	if vec, err = embedder(ctx, text); err != nil {
		return
	}
	opts := &redis.FTSearchOptions{
		Return:         ChatMessageDefaultReturn,
		DialectVersion: 2,
		Params:         map[string]interface{}{"vec": []byte(vector2string(vec))},
		SortBy: []redis.FTSearchSortBy{
			{FieldName: "score", Asc: true},
		},
	}
	filter := fmt.Sprintf("@%s:%s @%s:[%d inf]", key, val, ChatMessageTimestamp.As, from)
	query := fmt.Sprintf("(%s)=>[KNN %d @%s $vec AS %s]", filter, 1, ChatMessageContentVec.As, "score")
	cmd := history.redisCli.FTSearchWithArgs(ctx, history.indexName, query, opts)
	var result redis.FTSearchResult
	if result, err = cmd.Result(); err == nil && result.Total > 0 {
		for _, doc := range result.Docs {
			msg := parseChatMessage(&doc)
			msgs = append(msgs, msg)
		}
	}
	return
}

func (history *ChatHistory) delete(ctx context.Context, field string, value string) error {
	var keypattern string
	if field == ChatMessageSessionId.As {
		keypattern = fmt.Sprintf("%s:*:%s:*", history.docPrefix, value)
	} else if field == ChatMessageUserId.As {
		keypattern = fmt.Sprintf("%s:%s:*", history.docPrefix, value)
	} else {
		panic("unkown field name")
	}
	keys, err := history.redisCli.Keys(ctx, keypattern).Result()
	if err != nil {
		return err
	}
	if len(keys) > 0 {
		return history.redisCli.Del(ctx, keys...).Err()
	} else {
		return nil
	}
}

func parseChatMessage(doc *redis.Document) *ChatMessage {
	var chatMessage ChatMessage
	for key, val := range doc.Fields {
		switch key {
		case ChatMessageUserId.FieldName:
			chatMessage.UserId = val
		case ChatMessageSessionId.FieldName:
			chatMessage.SessionId = val
		case ChatMessageType.FieldName:
			chatMessage.Typ = val
		case ChatMessageContent.FieldName:
			chatMessage.Content = val
		case ChatMessageTimestamp.FieldName:
			timestamp, _ := strconv.ParseInt(val, 10, 64)
			chatMessage.Timestamp = timestamp
		}
	}
	return &chatMessage
}

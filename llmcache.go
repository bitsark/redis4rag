package redis4rag

import (
	"context"
	"crypto/md5"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/redis/go-redis/v9"
)

var (
	LLMCacheSchema []*redis.FieldSchema = []*redis.FieldSchema{
		QATag,
		QAQuery,
		QAAnswer,
		QAQueryVec,
	}

	QueryAnswerDefaultReturn = []redis.FTSearchReturn{
		{FieldName: QATag.FieldName},
		{FieldName: QAQuery.FieldName},
		{FieldName: QAAnswer.FieldName},
	}

	QATag = &redis.FieldSchema{
		FieldName: "$.tag", As: "tag", FieldType: redis.SearchFieldTypeTag, Separator: ",",
	}
	QAQuery = &redis.FieldSchema{
		FieldName: "$.query", As: "query", FieldType: redis.SearchFieldTypeText, NoStem: true,
	}
	QAAnswer = &redis.FieldSchema{
		FieldName: "$.answer", As: "answer", FieldType: redis.SearchFieldTypeText, NoIndex: true,
	}
	QAQueryVec = &redis.FieldSchema{
		FieldName: "$.query_vec", As: "query_vec", FieldType: redis.SearchFieldTypeVector,
		VectorArgs: &redis.FTVectorArgs{
			FlatOptions: &redis.FTFlatOptions{
				Type:           "FLOAT64",
				Dim:            1024,
				DistanceMetric: "COSINE",
			},
		}}
)

type (
	LLMsCache struct {
		indexName string
		docPrefix string
		redisCli  *redis.Client
	}

	QueryAnswer struct {
		Tag    string `json:"tag"`
		Query  string `json:"query"`
		Answer string `json:"answer"`
	}
)

func (cache *LLMsCache) Cache(ctx context.Context, qa *QueryAnswer, embedder Embedder) (err error) {
	var vec []float64
	if vec, err = embedder(ctx, qa.Query); err != nil {
		return
	}
	var jsonData []byte
	if jsonData, err = json.Marshal(qa); err != nil {
		return
	}
	pipeline := cache.redisCli.Pipeline()
	// key pattern: {LLMsCache.DocPrefix}:md5({QueryAnswer.Query})
	key := fmt.Sprintf("%s:%s", cache.docPrefix, makeCacheKey(qa.Query))
	pipeline.JSONSet(ctx, key, "$", string(jsonData))
	pipeline.JSONSet(ctx, key, QAQueryVec.FieldName, vec)
	_, err = pipeline.Exec(ctx)
	return err
}

func (cache *LLMsCache) Lookup(ctx context.Context, queryText string) (qa *QueryAnswer, err error) {
	opts := &redis.FTSearchOptions{
		Return:         QueryAnswerDefaultReturn,
		DialectVersion: 2,
	}
	query := fmt.Sprintf("@%s:%s", QAQuery.As, queryText)
	cmd := cache.redisCli.FTSearchWithArgs(ctx, cache.indexName, query, opts)
	var result redis.FTSearchResult
	if result, err = cmd.Result(); err == nil && result.Total > 0 {
		qa = parseQueryAnswer(&result.Docs[0])
	}
	return
}

func (cache *LLMsCache) SemanticSearch(ctx context.Context, tag string, queryText string, embedder Embedder) (qa *QueryAnswer, err error) {
	var vec []float64
	if vec, err = embedder(ctx, queryText); err != nil {
		return
	}
	opts := &redis.FTSearchOptions{
		Return:         QueryAnswerDefaultReturn,
		DialectVersion: 2,
		Params:         map[string]interface{}{"vec": []byte(vector2string(vec))},
		SortBy: []redis.FTSearchSortBy{
			{FieldName: "score", Asc: true},
		},
	}
	filter := fmt.Sprintf("@%s:{%s}", QATag.As, strings.ReplaceAll(tag, ",", "|"))
	query := fmt.Sprintf("(%s)=>[KNN %d @%s $vec AS %s]", filter, 1, QAQueryVec.As, "score")
	cmd := cache.redisCli.FTSearchWithArgs(ctx, cache.indexName, query, opts)
	var result redis.FTSearchResult
	if result, err = cmd.Result(); err == nil && result.Total > 0 {
		qa = parseQueryAnswer(&result.Docs[0])
	}
	return
}

func parseQueryAnswer(doc *redis.Document) *QueryAnswer {
	var qa QueryAnswer
	for key, val := range doc.Fields {
		switch key {
		case QATag.FieldName:
			qa.Tag = val
		case QAQuery.FieldName:
			qa.Query = val
		case QAAnswer.FieldName:
			qa.Answer = val
		}
	}
	return &qa
}

func makeCacheKey(text string) string {
	hash := md5.Sum([]byte(text))
	return hex.EncodeToString(hash[:])
}

package redis4rag

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/redis/go-redis/v9"
)

var (
	DocumentSchema []*redis.FieldSchema = []*redis.FieldSchema{
		DocId,
		DocTag,
		DocContent,
		DocPayload,
		DocContentVec,
	}

	DocumentDefaultReturn = []redis.FTSearchReturn{
		{FieldName: DocId.FieldName},
		{FieldName: DocTag.FieldName},
		{FieldName: DocContent.FieldName},
		{FieldName: DocPayload.FieldName},
	}

	DocId         = &redis.FieldSchema{FieldName: "$.id", As: "id", FieldType: redis.SearchFieldTypeText, NoIndex: true}
	DocTag        = &redis.FieldSchema{FieldName: "$.tag", As: "tag", FieldType: redis.SearchFieldTypeTag, Separator: ","}
	DocContent    = &redis.FieldSchema{FieldName: "$.content", As: "content", FieldType: redis.SearchFieldTypeText}
	DocPayload    = &redis.FieldSchema{FieldName: "$.payload", As: "payload", FieldType: redis.SearchFieldTypeText, NoIndex: true}
	DocContentVec = &redis.FieldSchema{FieldName: "$.content_vec", As: "content_vec", FieldType: redis.SearchFieldTypeVector,
		VectorArgs: &redis.FTVectorArgs{
			FlatOptions: &redis.FTFlatOptions{
				Type:           "FLOAT64",
				Dim:            1024,
				DistanceMetric: "COSINE",
			},
		}}
)

type (
	Retriever struct {
		indexName string
		docPrefix string
		redisCli  *redis.Client
	}

	Document struct {
		ID      string `json:"id"`
		Tag     string `json:"tag"`
		Content string `json:"content"`
		Payload string `json:"payload"`
	}
)

func (r *Retriever) Store(ctx context.Context, doc *Document, embedder Embedder) (err error) {
	var vec []float64
	vec, err = embedder(ctx, doc.Content)
	if err != nil {
		return
	}

	var jsonData []byte
	jsonData, err = json.Marshal(doc)
	if err != nil {
		return
	}

	// document key pattern: {Retriever.DocPrefix}:{Document.ID}
	pipeline := r.redisCli.Pipeline()
	key := fmt.Sprintf("%s:%s", r.docPrefix, doc.ID)
	pipeline.JSONSet(ctx, key, "$", string(jsonData))
	pipeline.JSONSet(ctx, key, DocContentVec.FieldName, vec)
	_, err = pipeline.Exec(ctx)
	return err
}

func (r *Retriever) Retrieve(ctx context.Context, content string, tag string, topK int, embedder Embedder) (docs []*Document, err error) {
	var vec []float64
	vec, err = embedder(ctx, content)
	if err != nil {
		return
	}

	opts := &redis.FTSearchOptions{
		Return:         DocumentDefaultReturn,
		DialectVersion: 2,
		Params:         map[string]interface{}{"vec": []byte(vector2string(vec))},
		SortBy: []redis.FTSearchSortBy{
			{FieldName: "score", Asc: true},
		},
	}

	filter := "*"
	if len(tag) > 0 {
		filter = fmt.Sprintf("@%s:{%s}", DocTag.As, strings.ReplaceAll(tag, ",", "|"))
	}
	query := fmt.Sprintf("(%s)=>[KNN %d @%s $vec AS %s]", filter, topK, DocContentVec.As, "score")
	cmd := r.redisCli.FTSearchWithArgs(ctx, r.indexName, query, opts)
	if res, err := cmd.Result(); err != nil {
		return docs, err
	} else if res.Total > 0 {
		for _, raw := range res.Docs {
			doc := parseDocument(&raw)
			docs = append(docs, doc)
		}
	}
	return
}

func parseDocument(res *redis.Document) *Document {
	var doc Document
	for key, val := range res.Fields {
		switch key {
		case DocId.FieldName:
			doc.ID = val
		case DocContent.FieldName:
			doc.Content = val
		case DocTag.FieldName:
			doc.Tag = val
		case DocPayload.FieldName:
			doc.Payload = val
		}
	}
	return &doc
}

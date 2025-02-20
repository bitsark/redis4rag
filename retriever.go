package redis4rag

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/redis/go-redis/v9"
)

type (
	Retriever struct {
		IndexName string
		DocPrefix string
		RedisCli  *redis.Client
	}

	Document struct {
		ID         string    `json:"id"`
		Tag        string    `json:"tag"`
		Content    string    `json:"content"`
		ContentVec []float64 `json:"contentvec"`
		Payload    string    `json:"payload"`
	}
)

const (
	field_id          = "$.id"
	field_tag         = "$.tag"
	field_content     = "$.content"
	field_content_vec = "$.contentvec"
	field_payload     = "$.payload"
	field_score       = "score"
)

var Schema []*redis.FieldSchema = []*redis.FieldSchema{
	{FieldName: field_id, As: "id", FieldType: redis.SearchFieldTypeText, NoIndex: true},
	{FieldName: field_tag, As: "tag", FieldType: redis.SearchFieldTypeTag, Separator: ","},
	{FieldName: field_content, As: "content", FieldType: redis.SearchFieldTypeText},
	{FieldName: field_payload, As: "payload", FieldType: redis.SearchFieldTypeText, NoIndex: true},
	{FieldName: field_content_vec, As: "contentvec", FieldType: redis.SearchFieldTypeVector,
		VectorArgs: &redis.FTVectorArgs{
			FlatOptions: &redis.FTFlatOptions{
				Type:           "FLOAT64",
				Dim:            1024,
				DistanceMetric: "COSINE",
			},
		}},
}

func (r *Retriever) Store(ctx context.Context, doc *Document, embedder Embedder) (err error) {
	doc.ContentVec, err = embedder(ctx, doc.Content)
	if err != nil {
		return err
	}

	buf, err := json.Marshal(doc)
	if err != nil {
		return err
	}

	// document key pattern: {Retriever.DocPrefix}:{Document.ID}
	return r.RedisCli.JSONSet(ctx,
		fmt.Sprintf("%s:%s", r.DocPrefix, doc.ID), "$", string(buf)).Err()
}

func (r *Retriever) Retrieve(ctx context.Context, content string, tag string, topK int, embedder Embedder) (docs []*Document, err error) {
	vec, err := embedder(ctx, content)
	if err != nil {
		return docs, err
	}

	opts := &redis.FTSearchOptions{
		Return: []redis.FTSearchReturn{
			{FieldName: field_id},
			{FieldName: field_tag},
			{FieldName: field_content},
			{FieldName: field_payload},
			{FieldName: field_score},
		},
		DialectVersion: 2,
		Params:         map[string]interface{}{"vec": []byte(vector2string(vec))},
		SortBy: []redis.FTSearchSortBy{
			{FieldName: field_score, Asc: true},
		},
	}

	filter := "*"
	if len(tag) > 0 {
		filter = fmt.Sprintf("@tag:{%s}", strings.ReplaceAll(tag, ",", "|"))
	}
	query := fmt.Sprintf("(%s)=>[KNN %d @contentvec $vec AS %s]", filter, topK, field_score)
	cmd := r.RedisCli.FTSearchWithArgs(ctx, r.IndexName, query, opts)
	if res, err := cmd.Result(); err != nil {
		return docs, err
	} else if res.Total > 0 {
		for _, raw := range res.Docs {
			doc := raw2doc(&raw)
			docs = append(docs, doc)
		}
	}
	return
}

func raw2doc(res *redis.Document) *Document {
	var doc Document
	for key, val := range res.Fields {
		switch key {
		case field_id:
			doc.ID = val
		case field_content:
			doc.Content = val
		case field_tag:
			doc.Tag = val
		case field_payload:
			doc.Payload = val
		}
	}
	return &doc
}

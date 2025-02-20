package redis4rag

import "context"

type Embedder func(context.Context, string) ([]float64, error)

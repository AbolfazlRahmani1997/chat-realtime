package entity

import "server/transform/api"

type Entity interface {
	ToTransformer() api.Transformer
	GetTableName() string
}

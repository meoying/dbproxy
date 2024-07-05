package sharding

import (
	"context"

	"github.com/meoying/dbproxy/internal/datasource/transaction"
)

func NewDelayTxContext(ctx context.Context) context.Context {
	return transaction.UsingTxType(ctx, transaction.Delay)
}

func NewSingleTxContext(ctx context.Context) context.Context {
	return transaction.UsingTxType(ctx, transaction.Single)
}

package sharding

import (
	"context"

	"github.com/meoying/dbproxy/internal/datasource"
	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/pcontext"
	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/visitor/builder"
	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/visitor/vparser"
	"github.com/meoying/dbproxy/internal/sharding"
)

type DeleteHandler struct {
	algorithm sharding.Algorithm
	db        datasource.DataSource
	deleteVal vparser.DeleteVal
	ctx       *pcontext.Context
	shardingBuilder
}

func NewDeleteHandler(a sharding.Algorithm, db datasource.DataSource, ctx *pcontext.Context) (ShardingHandler, error) {
	deleteVisitor := vparser.NewDeleteVisitor()
	resp := deleteVisitor.Parse(ctx.ParsedQuery.Root())
	baseVal := resp.(vparser.BaseVal)
	if baseVal.Err != nil {
		return nil, baseVal.Err
	}
	deleteVal := baseVal.Data.(vparser.DeleteVal)
	return &DeleteHandler{
		algorithm: a,
		deleteVal: deleteVal,
		db:        db,
		ctx:       ctx,
		shardingBuilder: shardingBuilder{
			algorithm: a,
		},
	}, nil
}

func (d *DeleteHandler) Build(ctx context.Context) ([]sharding.Query, error) {
	shardingRes, err := d.findDst(ctx, d.deleteVal.Predicate)
	if err != nil {
		return nil, err
	}
	res := make([]sharding.Query, 0, len(shardingRes.Dsts))
	for _, dst := range shardingRes.Dsts {
		deleteBuilder := builder.NewDelete(dst.DB, dst.Table)
		sql, err := deleteBuilder.Build(d.ctx.ParsedQuery.Root())
		if err != nil {
			return nil, err
		}
		res = append(res, sharding.Query{
			SQL:        sql,
			Table:      dst.Table,
			DB:         dst.DB,
			Datasource: dst.Name,
		})
	}
	return res, nil
}

func (d *DeleteHandler) QueryOrExec(ctx context.Context) (*Result, error) {
	qs, err := d.Build(ctx)
	if err != nil {
		return nil, err
	}
	res := exec(ctx, d.db, qs)
	return &Result{
		Result: res,
	}, nil
}

package sharding

import (
	"context"

	"github.com/meoying/dbproxy/internal/datasource"
	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/pcontext"
	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/visitor/builder"
	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/visitor/vparser"
	"github.com/meoying/dbproxy/internal/sharding"
)

type UpdateHandler struct {
	algorithm sharding.Algorithm
	db        datasource.DataSource
	updateVal vparser.UpdateVal
	ctx       *pcontext.Context
	shardingBuilder
}

func NewUpdateHandler(a sharding.Algorithm, db datasource.DataSource, ctx *pcontext.Context) (ShardingHandler, error) {
	updateVisitor := vparser.NewUpdateVisitor()
	resp := updateVisitor.Parse(ctx.ParsedQuery.Root())
	baseVal := resp.(vparser.BaseVal)
	if baseVal.Err != nil {
		return nil, baseVal.Err
	}
	updateVal := baseVal.Data.(vparser.UpdateVal)
	return &UpdateHandler{
		algorithm: a,
		updateVal: updateVal,
		db:        db,
		shardingBuilder: shardingBuilder{
			algorithm: a,
		},
		ctx: ctx,
	}, nil
}

func (u *UpdateHandler) Build(ctx context.Context) ([]sharding.Query, error) {
	shardingRes, err := u.findDst(ctx, u.updateVal.Predicate)
	if err != nil {
		return nil, err
	}
	res := make([]sharding.Query, 0, len(shardingRes.Dsts))
	for _, dst := range shardingRes.Dsts {
		updateBuilder := builder.NewUpdate(dst.DB, dst.Table)
		sql, err := updateBuilder.Build(u.ctx.ParsedQuery.Root())
		if err != nil {
			return nil, err
		}
		res = append(res, sharding.Query{
			SQL:        sql,
			DB:         dst.DB,
			Datasource: dst.Name,
		})
	}
	return res, nil
}

func (u *UpdateHandler) QueryOrExec(ctx context.Context) (*Result, error) {
	qs, err := u.Build(ctx)
	if err != nil {
		return nil, err
	}
	res := exec(ctx, u.db, qs)
	return &Result{
		Result: res,
	}, nil
}

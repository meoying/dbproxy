package sharding

import (
	"context"
	"github.com/meoying/dbproxy/internal/datasource"
	pcontext "github.com/meoying/dbproxy/internal/protocol/mysql/plugin/context"
	"github.com/meoying/dbproxy/internal/protocol/mysql/plugin/visitor"
	"github.com/meoying/dbproxy/internal/sharding"
	"github.com/valyala/bytebufferpool"
)

type UpdateHandler struct {
	algorithm sharding.Algorithm
	db        datasource.DataSource
	updateVal visitor.UpdateVal
	shardingBuilder
}

func NewUpdateHandler(a sharding.Algorithm, db datasource.DataSource, ctx *pcontext.Context) (*UpdateHandler, error) {
	updateVisitor := visitor.NewUpdateVisitor()
	resp := updateVisitor.Visit(ctx.ParsedQuery.Root)
	baseVal := resp.(visitor.BaseVal)
	if baseVal.Err != nil {
		return nil, baseVal.Err
	}
	updateVal := baseVal.Data.(visitor.UpdateVal)
	return &UpdateHandler{
		algorithm: a,
		updateVal: updateVal,
		db:        db,
		shardingBuilder: shardingBuilder{
			algorithm: a,
			builder: &builder{
				buffer: bytebufferpool.Get(),
			},
		},
	}, nil
}

func (u *UpdateHandler) Build(ctx context.Context) ([]sharding.Query, error) {
	shardingRes, err := u.findDst(ctx, u.updateVal.Predicate)
	if err != nil {
		return nil, err
	}

	res := make([]sharding.Query, 0, len(shardingRes.Dsts))
	defer bytebufferpool.Put(u.buffer)
	for _, dst := range shardingRes.Dsts {
		q, err := u.buildQuery(dst.DB, dst.Table, dst.Name)
		if err != nil {
			return nil, err
		}
		res = append(res, q)
		u.args = nil
		u.buffer.Reset()
	}
	return res, nil
}

func (u *UpdateHandler) buildQuery(db, tbl, ds string) (sharding.Query, error) {
	var err error
	u.writeString("UPDATE ")
	u.quote(db)
	u.writeByte('.')
	u.quote(tbl)
	u.writeString(" SET ")
	err = u.buildAssigns()
	if err != nil {
		return sharding.EmptyQuery, err
	}
	if u.updateVal.Predicate != (visitor.Predicate{}) {
		u.writeString(" WHERE ")
		if err = u.buildExpr(u.updateVal.Predicate); err != nil {
			return sharding.EmptyQuery, err
		}
	}
	u.end()

	return sharding.Query{SQL: u.buffer.String(), Args: u.args, Datasource: ds, DB: db}, nil
}

func (u *UpdateHandler) buildAssigns() error {
	has := false
	for _, assign := range u.updateVal.Assigns {
		if has {
			u.comma()
		}
		switch a := assign.(type) {
		case visitor.Assignment:
			if err := u.buildExpr(visitor.BinaryExpr(a)); err != nil {
				return err
			}
			has = true
		default:
			return ErrUnsupportedAssignment
		}
	}
	return nil
}

func (u *UpdateHandler) Exec(ctx context.Context) sharding.Result {
	qs, err := u.Build(ctx)
	if err != nil {
		return sharding.NewResult(nil, err)
	}
	return exec(ctx, u.db, qs)
}

package sharding

import (
	"context"
	"github.com/meoying/dbproxy/internal/datasource"
	pcontext "github.com/meoying/dbproxy/internal/protocol/mysql/plugin/context"
	"github.com/meoying/dbproxy/internal/protocol/mysql/plugin/visitor"
	"github.com/meoying/dbproxy/internal/protocol/mysql/plugin/visitor/vparser"
	"github.com/meoying/dbproxy/internal/sharding"
	"github.com/valyala/bytebufferpool"
)

type DeleteHandler struct {
	algorithm sharding.Algorithm
	db        datasource.DataSource
	deleteVal vparser.DeleteVal
	shardingBuilder
}

func NewDeleteHandler(a sharding.Algorithm, db datasource.DataSource, ctx *pcontext.Context) (*DeleteHandler, error) {
	deleteVisitor := vparser.NewDeleteVisitor()
	resp := deleteVisitor.Parse(ctx.ParsedQuery.Root)
	baseVal := resp.(vparser.BaseVal)
	if baseVal.Err != nil {
		return nil, baseVal.Err
	}
	deleteVal := baseVal.Data.(vparser.DeleteVal)
	return &DeleteHandler{
		algorithm: a,
		deleteVal: deleteVal,
		db:        db,
		shardingBuilder: shardingBuilder{
			algorithm: a,
			builder: &builder{
				buffer: bytebufferpool.Get(),
			},
		},
	}, nil
}

func (d *DeleteHandler) Build(ctx context.Context) ([]sharding.Query, error) {
	shardingRes, err := d.findDst(ctx, d.deleteVal.Predicate)
	if err != nil {
		return nil, err
	}
	res := make([]sharding.Query, 0, len(shardingRes.Dsts))
	defer bytebufferpool.Put(d.buffer)
	for _, dst := range shardingRes.Dsts {
		q, err := d.buildQuery(dst.DB, dst.Table, dst.Name)
		if err != nil {
			return nil, err
		}
		res = append(res, q)
		d.args = nil
		d.buffer.Reset()
	}
	return res, nil
}

func (d *DeleteHandler) buildQuery(db, tbl, ds string) (sharding.Query, error) {
	d.writeString("DELETE ")
	d.writeString(" FROM ")
	d.quote(db)
	d.writeByte('.')
	d.quote(tbl)
	if d.deleteVal.Predicate != (visitor.Predicate{}) {
		d.writeString(" WHERE ")
		if err := d.buildExpr(d.deleteVal.Predicate); err != nil {
			return sharding.EmptyQuery, err
		}
	}
	d.end()
	return sharding.Query{SQL: d.buffer.String(), Args: d.args, Datasource: ds, DB: db}, nil
}

func (d *DeleteHandler) Exec(ctx context.Context) sharding.Result {
	qs, err := d.Build(ctx)
	if err != nil {
		return sharding.NewResult(nil, err)
	}
	return exec(ctx, d.db, qs)
}

package sharding

import (
	"context"
	"errors"
	"github.com/ecodeclub/ekit/mapx"
	"github.com/meoying/dbproxy/internal/datasource"
	pcontext "github.com/meoying/dbproxy/internal/protocol/mysql/plugin/context"
	"github.com/meoying/dbproxy/internal/protocol/mysql/plugin/visitor"
	"github.com/meoying/dbproxy/internal/sharding"
	"github.com/meoying/dbproxy/internal/sharding/operator"
	"github.com/valyala/bytebufferpool"
)

var ErrInsertFindingDst = errors.New(" 一行数据只能插入一个表")

type InsertHandler struct {
	insertVal visitor.InsertVal
	algorithm sharding.Algorithm
	db        datasource.DataSource
	*builder
}

func NewInsertBuilder(a sharding.Algorithm, db datasource.DataSource, ctx *pcontext.Context) (*InsertHandler, error) {
	insertVisitor := visitor.NewInsertVisitor()
	resp := insertVisitor.Visit(ctx.ParsedQuery.Root)
	baseVal := resp.(visitor.BaseVal)
	if baseVal.Err != nil {
		return nil, baseVal.Err
	}
	insertVal := baseVal.Data.(visitor.InsertVal)
	return &InsertHandler{
		algorithm: a,
		insertVal: insertVal,
		db:        db,
		builder: &builder{
			buffer: bytebufferpool.Get(),
			args:   make([]any, 0, 16),
		},
	}, nil
}

func (i *InsertHandler) Build(ctx context.Context) ([]sharding.Query, error) {
	dsDBTabMap, err := mapx.NewMultiTreeMap[sharding.Dst, visitor.ValMap](sharding.CompareDSDBTab)
	if err != nil {
		return nil, err
	}
	if err := i.checkColumns(i.insertVal.Cols, i.algorithm.ShardingKeys()); err != nil {
		return nil, err
	}
	for _, value := range i.insertVal.Vals {
		dst, err := i.getDst(ctx, value)
		if err != nil {
			return nil, err
		}
		if len(dst.Dsts) != 1 {
			return nil, ErrInsertFindingDst
		}
		err = dsDBTabMap.Put(dst.Dsts[0], value)
		if err != nil {
			return nil, err
		}
	}
	dsts := dsDBTabMap.Keys()

	ansQuery := make([]sharding.Query, 0, len(dsts))
	for _, dst := range dsts {
		vals, _ := dsDBTabMap.Get(dst)
		err = i.buildQuery(dst.DB, dst.Table, i.insertVal.Cols, vals)
		if err != nil {
			return nil, err
		}
		ansQuery = append(ansQuery, sharding.Query{
			SQL:        i.buffer.String(),
			Args:       i.args,
			DB:         dst.DB,
			Datasource: dst.Name,
		})
		i.buffer.Reset()
		i.args = []any{}
	}
	return ansQuery, nil
}

func (i *InsertHandler) buildQuery(db, table string, cols []string, values []visitor.ValMap) error {
	var err error
	i.writeString("INSERT INTO ")
	i.quote(db)
	i.writeByte('.')
	i.quote(table)
	i.writeString("(")
	err = i.buildColumns(cols)
	if err != nil {
		return err
	}
	i.writeString(")")
	i.writeString(" VALUES")
	for index, valMap := range values {
		if index > 0 {
			i.comma()
		}
		i.writeString("(")
		for j, v := range cols {
			i.parameter(valMap[v].Val)
			if j != len(cols)-1 {
				i.comma()
			}
		}
		i.writeString(")")
	}
	i.end()
	return nil
}

func (i *InsertHandler) buildColumns(colMetas []string) error {
	for idx, colMeta := range colMetas {
		i.quote(colMeta)
		if idx != len(colMetas)-1 {
			i.comma()
		}
	}
	return nil
}

func (i *InsertHandler) getDst(ctx context.Context, valMap visitor.ValMap) (sharding.Response, error) {
	sks := i.algorithm.ShardingKeys()
	skValues := make(map[string]any)
	for _, sk := range sks {
		col := valMap[sk]
		skValues[sk] = col.Val
	}
	return i.algorithm.Sharding(ctx, sharding.Request{
		Op:       operator.OpEQ,
		SkValues: skValues,
	})
}

func (i *InsertHandler) Exec(ctx context.Context) sharding.Result {
	qs, err := i.Build(ctx)
	if err != nil {
		return sharding.NewResult(nil, err)
	}
	return exec(ctx,i.db,qs)
}

// checkColumns 判断sk是否存在于meta中，如果不存在会返回报错
func (i *InsertHandler) checkColumns(cols []string, sks []string) error {
	colMetasMap := make(map[string]struct{}, len(cols))
	for _, col := range cols {
		colMetasMap[col] = struct{}{}
	}
	for _, sk := range sks {
		if _, ok := colMetasMap[sk]; !ok {
			return ErrInsertShardingKeyNotFound
		}
	}
	return nil
}

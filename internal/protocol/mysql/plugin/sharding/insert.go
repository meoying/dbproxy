package sharding

import (
	"context"

	"github.com/ecodeclub/ekit/mapx"
	"github.com/meoying/dbproxy/internal/datasource"
	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/ast/parser"
	"github.com/meoying/dbproxy/internal/protocol/mysql/plugin"
	pcontext "github.com/meoying/dbproxy/internal/protocol/mysql/plugin/context"
	vbuilder "github.com/meoying/dbproxy/internal/protocol/mysql/plugin/visitor/builder"
	"github.com/meoying/dbproxy/internal/protocol/mysql/plugin/visitor/vparser"
	"github.com/meoying/dbproxy/internal/sharding"
	"github.com/meoying/dbproxy/internal/sharding/operator"
	"github.com/pkg/errors"
)

var ErrInsertFindingDst = errors.New(" 一行数据只能插入一个表")

type InsertHandler struct {
	insertVal vparser.InsertVal
	algorithm sharding.Algorithm
	db        datasource.DataSource
	ctx       *pcontext.Context
}

func (i *InsertHandler) QueryOrExec(ctx context.Context) (*plugin.Result, error) {
	qs, err := i.Build(ctx)
	if err != nil {
		return nil, err
	}
	res := exec(ctx, i.db, qs)
	return &plugin.Result{
		Result: res,
	}, res.Err()
}

func NewInsertBuilder(a sharding.Algorithm, db datasource.DataSource, ctx *pcontext.Context) (ShardingHandler, error) {
	insertVisitor := vparser.NewInsertVisitor()
	resp := insertVisitor.Parse(ctx.ParsedQuery.Root)
	baseVal := resp.(vparser.BaseVal)
	if baseVal.Err != nil {
		return nil, baseVal.Err
	}
	insertVal := baseVal.Data.(vparser.InsertVal)
	return &InsertHandler{
		algorithm: a,
		insertVal: insertVal,
		db:        db,
		ctx:       ctx,
	}, nil
}

func (i *InsertHandler) Build(ctx context.Context) ([]sharding.Query, error) {
	dsDBTabMap, err := mapx.NewMultiTreeMap[sharding.Dst, *parser.ExpressionsWithDefaultsContext](sharding.CompareDSDBTab)
	if err != nil {
		return nil, err
	}
	if err := i.checkColumns(i.insertVal.Cols, i.algorithm.ShardingKeys()); err != nil {
		return nil, err
	}
	for idx, value := range i.insertVal.Vals {
		dst, err := i.getDst(ctx, value)
		if err != nil {
			return nil, err
		}
		if len(dst.Dsts) != 1 {
			return nil, ErrInsertFindingDst
		}
		err = dsDBTabMap.Put(dst.Dsts[0], i.insertVal.AstValues[idx])
		if err != nil {
			return nil, err
		}
	}
	dsts := dsDBTabMap.Keys()
	ansQuery := make([]sharding.Query, 0, len(dsts))
	for _, dst := range dsts {
		vals, _ := dsDBTabMap.Get(dst)
		insertBuilder := vbuilder.NewInsert(dst.DB, dst.Table, vals)
		sql, err := insertBuilder.Build(i.ctx.ParsedQuery.Root)
		if err != nil {
			return nil, err
		}
		ansQuery = append(ansQuery, sharding.Query{
			SQL:        sql,
			DB:         dst.DB,
			Datasource: dst.Name,
		})
	}
	return ansQuery, nil
}

func (i *InsertHandler) getDst(ctx context.Context, valMap vparser.ValMap) (sharding.Response, error) {
	sks := i.algorithm.ShardingKeys()
	skValues := make(map[string]any)
	for _, sk := range sks {
		skValues[sk] = valMap[sk]
	}
	return i.algorithm.Sharding(ctx, sharding.Request{
		Op:       operator.OpEQ,
		SkValues: skValues,
	})
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

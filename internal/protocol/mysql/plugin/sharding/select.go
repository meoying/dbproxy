package sharding

import (
	"context"
	"fmt"
	"strings"

	"github.com/ecodeclub/ekit/list"
	"github.com/ecodeclub/ekit/sqlx"
	_ "github.com/mattn/go-sqlite3"
	"github.com/meoying/dbproxy/internal/datasource"
	"github.com/meoying/dbproxy/internal/datasource/masterslave"
	"github.com/meoying/dbproxy/internal/merger"
	"github.com/meoying/dbproxy/internal/merger/factory"
	"github.com/meoying/dbproxy/internal/protocol/mysql/plugin"
	pcontext "github.com/meoying/dbproxy/internal/protocol/mysql/plugin/context"
	"github.com/meoying/dbproxy/internal/protocol/mysql/plugin/visitor"
	visitorBuilder "github.com/meoying/dbproxy/internal/protocol/mysql/plugin/visitor/builder"
	"github.com/meoying/dbproxy/internal/protocol/mysql/plugin/visitor/vparser"
	"github.com/meoying/dbproxy/internal/query"
	"github.com/meoying/dbproxy/internal/sharding"
	"github.com/pkg/errors"
	"golang.org/x/sync/errgroup"
)

var ErrUnsupportedTooComplexQuery = errors.New("暂未支持太复杂的查询")

func NewUnsupportedOperatorError(op string) error {
	return fmt.Errorf("不支持的 operator %v", op)
}

type SelectHandler struct {
	algorithm sharding.Algorithm
	db        datasource.DataSource
	selectVal vparser.SelectVal
	ctx       *pcontext.Context
	shardingBuilder
}

func (s *SelectHandler) Build(ctx context.Context) ([]sharding.Query, error) {
	var err error
	shardingRes, err := s.findDst(ctx, s.selectVal.Predicate)
	if err != nil {
		return nil, err
	}
	res := make([]sharding.Query, 0, len(shardingRes.Dsts))
	for idx, dst := range shardingRes.Dsts {
		var selectBuilder *visitorBuilder.Select
		opts := make([]visitorBuilder.SelectOption, 0, 4)
		if s.selectVal.LimitClause != nil {
			limit := s.selectVal.LimitClause.Limit
			offset := s.selectVal.LimitClause.Offset
			opts = append(opts, visitorBuilder.WithLimit(limit+offset, 0))
		}
		if idx > 0 {
			opts = append(opts, visitorBuilder.WithChanged())
		}
		selectBuilder = visitorBuilder.NewSelect(dst.DB, dst.Table, opts...)
		sql, err := selectBuilder.Build(s.ctx.ParsedQuery.Root)
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

func (s *SelectHandler) QueryOrExec(ctx context.Context) (*plugin.Result, error) {
	qs, err := s.Build(ctx)
	if err != nil {
		return nil, err
	}
	originCols, targetCols, err := s.newQuerySpec()
	if err != nil {
		return nil, err
	}
	mgr, err := factory.New(originCols, targetCols)
	if err != nil {
		return nil, err
	}
	rowsList, err := s.queryMulti(ctx, qs)
	if err != nil {
		return nil, err
	}
	rows, err := mgr.Merge(ctx, rowsList.AsSlice())
	if err != nil {
		return nil, err
	}
	return &plugin.Result{
		Rows: rows,
	}, nil
}

func (s *SelectHandler) newQuerySpec() (factory.QuerySpec, factory.QuerySpec, error) {
	var originSpec, targetSpec factory.QuerySpec
	var hasAgg, hasGroupBy, hasOrderBy bool
	var err error
	features := make([]query.Feature, 0, 8)
	originSpec.Select, targetSpec.Select, hasAgg = s.newSelect()
	if hasAgg {
		features = append(features, query.AggregateFunc)
	}
	originSpec.GroupBy, targetSpec.GroupBy, hasGroupBy, err = s.newGroupBy()
	if err != nil {
		return originSpec, targetSpec, err
	}
	if hasGroupBy {
		features = append(features, query.GroupBy)
	}
	originSpec.OrderBy, targetSpec.OrderBy, hasOrderBy, err = s.newOrderBy()
	if err != nil {
		return originSpec, targetSpec, err
	}
	if hasOrderBy {
		features = append(features, query.OrderBy)
	}
	if s.selectVal.LimitClause != nil {
		features = append(features, query.Limit)
		originSpec.Limit = s.selectVal.LimitClause.Limit
		originSpec.Offset = s.selectVal.LimitClause.Offset
		targetSpec.Offset = 0
		targetSpec.Limit = s.selectVal.LimitClause.Limit + s.selectVal.LimitClause.Offset
	}
	if s.selectVal.Distinct {
		originSpec = s.setDistinct(originSpec)
		targetSpec = s.setDistinct(targetSpec)
		features = append(features, query.Distinct)
	}
	originSpec.Features = features
	targetSpec.Features = features
	return originSpec, targetSpec, nil
}

func (s *SelectHandler) setDistinct(originSpec factory.QuerySpec) factory.QuerySpec {
	for idx := range originSpec.Select {
		originSpec.Select[idx].Distinct = true
	}
	return originSpec
}

func (s *SelectHandler) newOrderBy() ([]merger.ColumnInfo, []merger.ColumnInfo, bool, error) {
	originCols := make([]merger.ColumnInfo, 0, len(s.selectVal.Cols))
	targetCols := make([]merger.ColumnInfo, 0, len(s.selectVal.Cols))
	for _, orderCol := range s.selectVal.OrderClauses {
		colInfo, err := s.findCol(orderCol.Column)
		if err != nil {
			return nil, nil, false, err
		}
		if strings.ToUpper(orderCol.Order) == "ASC" {
			colInfo.Order = merger.OrderASC
		} else {
			colInfo.Order = merger.OrderDESC
		}
		originCols = append(originCols, colInfo)
		targetCols = append(targetCols, colInfo)
	}
	return originCols, targetCols, len(s.selectVal.OrderClauses) > 0, nil
}

func (s *SelectHandler) newSelect() ([]merger.ColumnInfo, []merger.ColumnInfo, bool) {
	var originIndex, targetIndex int
	originCols := make([]merger.ColumnInfo, 0, len(s.selectVal.Cols))
	targetCols := make([]merger.ColumnInfo, 0, len(s.selectVal.Cols))
	var hasAggregate bool
	for _, col := range s.selectVal.Cols {
		switch selectCol := col.(type) {
		case visitor.Column:
			originCols = append(originCols, merger.ColumnInfo{
				Index: originIndex,
				Name:  selectCol.Name,
				Alias: selectCol.Alias,
			})
			targetCols = append(targetCols, merger.ColumnInfo{
				Index: targetIndex,
				Name:  selectCol.Name,
				Alias: selectCol.Alias,
			})
			originIndex++
			targetIndex++
		case visitor.Aggregate:
			hasAggregate = true
			// todo 补充处理聚合函数的方法
			originCols = append(originCols, merger.ColumnInfo{
				Index:         originIndex,
				Name:          selectCol.Arg,
				Alias:         selectCol.Alias,
				AggregateFunc: selectCol.Fn,
				Distinct:      selectCol.Distinct,
			})
			originIndex++
			targetCols = append(targetCols, merger.ColumnInfo{
				Index:         targetIndex,
				Name:          selectCol.Arg,
				Alias:         selectCol.Alias,
				AggregateFunc: selectCol.Fn,
				Distinct:      selectCol.Distinct,
			})
			targetIndex++
			// 如果是AVG,需要再添加 COUNT 和 SUM
			if strings.ToUpper(selectCol.Fn) == "AVG" {
				sumInfo := merger.ColumnInfo{
					Index:         targetIndex,
					Name:          selectCol.Arg,
					AggregateFunc: "SUM",
					Distinct:      selectCol.Distinct,
				}
				targetCols = append(targetCols, sumInfo)
				targetIndex++
				countInfo := merger.ColumnInfo{
					Index:         targetIndex,
					Name:          selectCol.Arg,
					AggregateFunc: "COUNT",
					Distinct:      selectCol.Distinct,
				}
				targetCols = append(targetCols, countInfo)
			}
		}
	}
	return originCols, targetCols, hasAggregate
}

func (s *SelectHandler) newGroupBy() ([]merger.ColumnInfo, []merger.ColumnInfo, bool, error) {
	originCols := make([]merger.ColumnInfo, 0, len(s.selectVal.Cols))
	targetCols := make([]merger.ColumnInfo, 0, len(s.selectVal.Cols))
	for _, groupByCol := range s.selectVal.GroupByClause {
		colInfo, err := s.findCol(groupByCol)
		if err != nil {
			return nil, nil, false, err
		}
		originCols = append(originCols, colInfo)
		targetCols = append(targetCols, colInfo)
	}
	return originCols, targetCols, len(s.selectVal.GroupByClause) > 0, nil
}

func NewSelectHandler(a sharding.Algorithm, db datasource.DataSource, ctx *pcontext.Context) (ShardingHandler, error) {
	selectVisitor := vparser.NewsSelectVisitor()
	hintVisitor := vparser.NewHintVisitor()
	hint := hintVisitor.Visit(ctx.ParsedQuery.Root)
	if strings.Contains(hint.(string), "useMaster") {
		ctx.Context = masterslave.UseMaster(ctx.Context)
	}
	resp := selectVisitor.Parse(ctx.ParsedQuery.Root)
	baseVal := resp.(vparser.BaseVal)
	if baseVal.Err != nil {
		return nil, baseVal.Err
	}
	selectVal := baseVal.Data.(vparser.SelectVal)
	return &SelectHandler{
		algorithm: a,
		selectVal: selectVal,
		db:        db,
		ctx:       ctx,
		shardingBuilder: shardingBuilder{
			algorithm: a,
		},
	}, nil
}
func (s *SelectHandler) findCol(name string) (merger.ColumnInfo, error) {
	for idx, selectCol := range s.selectVal.Cols {
		var selectColInfo merger.ColumnInfo
		var selectName string
		switch v := selectCol.(type) {
		case visitor.Column:
			selectName = v.Name
			if v.Alias != "" {
				selectName = v.Alias
			}
			selectColInfo = merger.ColumnInfo{
				Index: idx,
				Name:  v.Name,
				Alias: v.Alias,
			}
		case visitor.Aggregate:
			if v.Alias != "" {
				selectName = v.Alias
			}
			selectColInfo = merger.ColumnInfo{
				Index:         idx,
				Name:          v.Arg,
				AggregateFunc: v.Fn,
				Alias:         v.Alias,
				Distinct:      v.Distinct,
			}
		}
		if name == selectName {
			return selectColInfo, nil
		}
	}
	return merger.ColumnInfo{}, NewErrUnKnowSelectCol(name)

}

func (s *SelectHandler) queryMulti(ctx context.Context, qs []sharding.Query) (list.List[sqlx.Rows], error) {
	res := &list.ConcurrentList[sqlx.Rows]{
		List: list.NewArrayList[sqlx.Rows](len(qs)),
	}
	var eg errgroup.Group
	for _, query := range qs {
		q := query
		eg.Go(func() error {
			rs, err := s.db.Query(ctx, q)
			if err == nil {
				return res.Append(rs)
			}
			return err
		})
	}
	return res, eg.Wait()
}

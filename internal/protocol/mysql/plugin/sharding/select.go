package sharding

import (
	"context"
	"fmt"
	"github.com/ecodeclub/ekit/list"
	"github.com/ecodeclub/ekit/sqlx"
	"github.com/meoying/dbproxy/internal/datasource"
	"github.com/meoying/dbproxy/internal/datasource/masterslave"
	"github.com/meoying/dbproxy/internal/protocol/mysql/plugin"
	pcontext "github.com/meoying/dbproxy/internal/protocol/mysql/plugin/context"
	"github.com/meoying/dbproxy/internal/protocol/mysql/plugin/sharding/merger/batchmerger"
	visitorBuilder "github.com/meoying/dbproxy/internal/protocol/mysql/plugin/visitor/builder"
	"github.com/meoying/dbproxy/internal/protocol/mysql/plugin/visitor/vparser"
	"github.com/meoying/dbproxy/internal/sharding"
	"github.com/pkg/errors"
	"github.com/valyala/bytebufferpool"
	"golang.org/x/sync/errgroup"
	"strings"
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
	for _, dst := range shardingRes.Dsts {
		sql, err := visitorBuilder.NewSelect(dst.DB, dst.Table).Build(s.ctx.ParsedQuery.Root)
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

	mgr := batchmerger.NewMerger()
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
			builder: &builder{
				buffer: bytebufferpool.Get(),
			},
		},
	}, nil
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

//func (s *SelectHandler) buildQuery(db, tbl, ds string) (sharding.Query, error) {
//	var err error
//	s.writeString("SELECT ")
//	if s.selectVal.Distinct {
//		s.writeString("DISTINCT ")
//	}
//	if len(s.selectVal.Cols) == 0 {
//		s.builder.writeString("*")
//	} else {
//		err = s.buildSelectedList()
//		if err != nil {
//			return sharding.EmptyQuery, err
//		}
//	}
//
//	s.writeString(" FROM ")
//	s.quote(db)
//	s.writeByte('.')
//	s.quote(tbl)
//	if s.selectVal.Predicate != (visitor.Predicate{}) {
//		s.writeString(" WHERE ")
//		if err = s.buildExpr(s.selectVal.Predicate); err != nil {
//			return sharding.EmptyQuery, err
//		}
//	}
//	// group by
//	if len(s.selectVal.GroupByClause) > 0 {
//		err = s.buildGroupBy()
//		if err != nil {
//			return sharding.EmptyQuery, err
//		}
//	}
//	// order by
//	if len(s.selectVal.OrderClauses) > 0 {
//		err = s.buildOrderBy()
//		if err != nil {
//			return sharding.EmptyQuery, err
//		}
//	}
//	// limit
//	s.buildLimit()
//	s.end()
//	return sharding.Query{SQL: s.buffer.String(), Args: s.args, Datasource: ds, DB: db}, nil
//}
//
//func (s *SelectHandler) buildColumns(index int, name string) error {
//	if index > 0 {
//		s.comma()
//	}
//	s.quote(name)
//	return nil
//}
//
//func (s *SelectHandler) buildSelectedList() error {
//	for i, col := range s.selectVal.Cols {
//		if i > 0 {
//			s.comma()
//		}
//		var err error
//		switch expr := col.(type) {
//		case visitor.Column:
//			err = s.builder.buildColumn(expr)
//		case visitor.Aggregate:
//			err = s.selectAggregate(expr)
//		}
//		if err != nil {
//			return err
//		}
//	}
//	return nil
//
//}
//
//func (s *SelectHandler) selectAggregate(aggregate visitor.Aggregate) error {
//	// 如果是AVG需要转化成SUM 和 COUNT
//	if aggregate.Fn == "AVG" {
//		aggregate.Fn = "SUM"
//		err := s.selectAggregate(aggregate)
//		if err != nil {
//			return err
//		}
//		s.comma()
//		aggregate.Fn = "COUNT"
//		err = s.selectAggregate(aggregate)
//		return err
//	}
//	s.writeString(aggregate.Fn)
//	s.writeByte('(')
//	if aggregate.Distinct {
//		s.writeString("DISTINCT ")
//	}
//	s.writeString(aggregate.Arg)
//	s.writeByte(')')
//	if aggregate.Alias != "" {
//		s.writeString(" AS ")
//		s.quote(aggregate.Alias)
//	}
//	return nil
//}
//
//func (s *SelectHandler) buildGroupBy() error {
//	s.writeString(" GROUP BY ")
//	for i, gb := range s.selectVal.GroupByClause {
//		if i > 0 {
//			s.comma()
//		}
//		s.quote(gb)
//	}
//	return nil
//}
//
//func (s *SelectHandler) buildOrderBy() error {
//	s.writeString(" ORDER BY ")
//	for i, ob := range s.selectVal.OrderClauses {
//		if i > 0 {
//			s.comma()
//		}
//		s.quote(ob.Column)
//		s.space()
//		s.writeString(ob.Order)
//	}
//	return nil
//}
//
//func (s *SelectHandler) buildLimit() {
//	if s.selectVal.LimitClause != nil {
//		limit := s.selectVal.LimitClause.Limit + s.selectVal.LimitClause.Offset
//		s.writeString(" LIMIT ")
//		s.parameter(limit)
//	}
//}

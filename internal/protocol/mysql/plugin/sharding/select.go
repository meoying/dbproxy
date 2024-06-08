package sharding

import (
	"context"
	"fmt"
	"github.com/ecodeclub/ekit/list"
	"github.com/ecodeclub/ekit/sqlx"
	"github.com/meoying/dbproxy/internal/datasource"
	pcontext "github.com/meoying/dbproxy/internal/protocol/mysql/plugin/context"
	"github.com/meoying/dbproxy/internal/protocol/mysql/plugin/sharding/merger/batchmerger"
	"github.com/meoying/dbproxy/internal/protocol/mysql/plugin/visitor"
	"github.com/meoying/dbproxy/internal/sharding"
	"github.com/pkg/errors"
	"github.com/valyala/bytebufferpool"
	"golang.org/x/sync/errgroup"
)

var ErrUnsupportedTooComplexQuery = errors.New("暂未支持太复杂的查询")

func NewUnsupportedOperatorError(op string) error {
	return fmt.Errorf("不支持的 operator %v", op)
}

type SelectHandler struct {
	algorithm sharding.Algorithm
	db        datasource.DataSource
	selectVal visitor.SelectVal
	shardingBuilder
}

func NewSelectHandler(a sharding.Algorithm, db datasource.DataSource, ctx *pcontext.Context) (*SelectHandler, error) {
	selectVisitor := visitor.NewsSelectVisitor()
	resp := selectVisitor.Visit(ctx.ParsedQuery.Root)
	baseVal := resp.(visitor.BaseVal)
	if baseVal.Err != nil {
		return nil, baseVal.Err
	}
	selectVal := baseVal.Data.(visitor.SelectVal)
	return &SelectHandler{
		algorithm: a,
		selectVal: selectVal,
		db:        db,
		shardingBuilder: shardingBuilder{
			algorithm: a,
			builder: &builder{
				buffer: bytebufferpool.Get(),
			},
		},
	}, nil
}

func (s *SelectHandler) Build(ctx context.Context) ([]sharding.Query, error) {
	var err error

	shardingRes, err := s.findDst(ctx, s.selectVal.Predicate)
	if err != nil {
		return nil, err
	}
	res := make([]sharding.Query, 0, len(shardingRes.Dsts))
	defer bytebufferpool.Put(s.buffer)
	for _, dst := range shardingRes.Dsts {
		q, err := s.buildQuery(dst.DB, dst.Table, dst.Name)
		if err != nil {
			return nil, err
		}
		res = append(res, q)
		s.args = nil
		s.buffer.Reset()
	}
	return res, nil
}

func (s *SelectHandler) GetMulti(ctx context.Context) (sqlx.Rows, error) {
	qs, err := s.Build(ctx)
	if err != nil {
		return nil, err
	}

	mgr := batchmerger.NewMerger()
	rowsList, err := s.queryMulti(ctx, qs)
	if err != nil {
		return nil, err
	}
	return mgr.Merge(ctx, rowsList.AsSlice())
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

func (s *SelectHandler) buildQuery(db, tbl, ds string) (sharding.Query, error) {
	var err error
	s.writeString("SELECT ")
	if s.selectVal.Distinct {
		s.writeString("DISTINCT ")
	}
	if len(s.selectVal.Cols) == 0 {
		s.builder.writeString("*")
	} else {
		err = s.buildSelectedList()
		if err != nil {
			return sharding.EmptyQuery, err
		}
	}

	s.writeString(" FROM ")
	s.quote(db)
	s.writeByte('.')
	s.quote(tbl)
	if s.selectVal.Predicate != (visitor.Predicate{}) {
		s.writeString(" WHERE ")
		if err = s.buildExpr(s.selectVal.Predicate); err != nil {
			return sharding.EmptyQuery, err
		}
	}
	s.end()
	return sharding.Query{SQL: s.buffer.String(), Args: s.args, Datasource: ds, DB: db}, nil
}

func (s *SelectHandler) buildColumns(index int, name string) error {
	if index > 0 {
		s.comma()
	}
	s.quote(name)
	return nil
}

func (s *SelectHandler) buildSelectedList() error {
	for i, col := range s.selectVal.Cols {
		if i > 0 {
			s.comma()
		}
		var err error
		switch expr := col.(type) {
		case visitor.Column:
			err = s.builder.buildColumn(expr)
		case visitor.Aggregate:
			err = s.selectAggregate(expr)
		}
		if err != nil {
			return err
		}
	}
	return nil

}

func (s *SelectHandler) selectAggregate(aggregate visitor.Aggregate) error {
	// 如果是AVG需要转化成SUM 和 COUNT
	if aggregate.Fn == "AVG" {
		aggregate.Fn = "SUM"
		err := s.selectAggregate(aggregate)
		if err != nil {
			return err
		}
		s.comma()
		aggregate.Fn = "COUNT"
		err = s.selectAggregate(aggregate)
		return err
	}
	s.writeString(aggregate.Fn)
	s.writeByte('(')
	if aggregate.Distinct {
		s.writeString("DISTINCT ")
	}
	s.writeString(aggregate.Arg)
	s.writeByte(')')
	if aggregate.Alias != "" {
		s.writeString(" AS ")
		s.quote(aggregate.Alias)
	}
	return nil
}

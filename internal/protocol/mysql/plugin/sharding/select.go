package sharding

import (
	"context"
	"fmt"
	"github.com/ecodeclub/ekit/list"
	"github.com/ecodeclub/ekit/slice"
	"github.com/ecodeclub/ekit/sqlx"
	"github.com/meoying/dbproxy/internal/datasource"
	pcontext "github.com/meoying/dbproxy/internal/protocol/mysql/plugin/context"
	"github.com/meoying/dbproxy/internal/protocol/mysql/plugin/sharding/merger/batchmerger"
	"github.com/meoying/dbproxy/internal/protocol/mysql/plugin/visitor"
	"github.com/meoying/dbproxy/internal/sharding"
	"github.com/meoying/dbproxy/internal/sharding/operator"
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
	*builder
}

func NewSelectHandler(a sharding.Algorithm, db datasource.DataSource, ctx *pcontext.Context) (*SelectHandler, error) {
	visitors := ctx.Visitors
	// 获取insert，visitor
	selectVisitor, ok := visitors["sharding_SelectVisitor"]
	if !ok {
		return nil, errors.New("SelectVisitor未找到")
	}
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
		builder: &builder{
			buffer: bytebufferpool.Get(),
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

func (s *SelectHandler) buildExpr(expr visitor.Expr) error {
	switch exp := expr.(type) {
	case nil:
	case visitor.Column:
		exp.Alias = ""
		_ = s.buildColumn(exp)
	case visitor.ValueExpr:
		s.parameter(exp.Val)
	case visitor.RawExpr:
		s.buildRawExpr(exp)
	case visitor.Predicate:
		if err := s.buildBinaryExpr(visitor.BinaryExpr(exp)); err != nil {
			return err
		}
	default:
		return NewErrUnsupportedExpressionType
	}
	return nil
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
		err := s.builder.buildColumn(col)
		if err != nil {
			return err
		}
	}
	return nil

}

func (s *SelectHandler) findDst(ctx context.Context, predicate visitor.Predicate) (sharding.Response, error) {
	if predicate != (visitor.Predicate{}) {
		return s.findDstByPredicate(ctx, predicate)
	}
	return sharding.Response{
		Dsts: s.algorithm.Broadcast(ctx),
	},nil
}

func (b *SelectHandler) findDstByPredicate(ctx context.Context, pre visitor.Predicate) (sharding.Response, error) {
	switch pre.Op {
	case operator.OpAnd:
		left, err := b.findDstByPredicate(ctx, pre.Left.(visitor.Predicate))
		if err != nil {
			return sharding.EmptyResp, err
		}
		right, err := b.findDstByPredicate(ctx, pre.Right.(visitor.Predicate))
		if err != nil {
			return sharding.EmptyResp, err
		}
		return b.mergeAnd(left, right), nil
	case operator.OpOr:
		left, err := b.findDstByPredicate(ctx, pre.Left.(visitor.Predicate))
		if err != nil {
			return sharding.EmptyResp, err
		}
		right, err := b.findDstByPredicate(ctx, pre.Right.(visitor.Predicate))
		if err != nil {
			return sharding.EmptyResp, err
		}
		return b.mergeOR(left, right), nil
	case operator.OpIn:
		col := pre.Left.(visitor.Column)
		right := pre.Right.(visitor.Values)
		var results []sharding.Response
		for _, val := range right.Vals {
			res, err := b.algorithm.Sharding(ctx,
				sharding.Request{Op: operator.OpEQ, SkValues: map[string]any{col.Name: val}})
			if err != nil {
				return sharding.EmptyResp, err
			}
			results = append(results, res)
		}
		return b.mergeIN(results), nil
	case operator.OpNot:
		nPre, err := b.negatePredicate(pre.Right.(visitor.Predicate))
		if err != nil {
			return sharding.EmptyResp, err
		}
		return b.findDstByPredicate(ctx, nPre)
	case operator.OpNotIN:
		return b.algorithm.Sharding(ctx,
			sharding.Request{Op: operator.OpNotIN, SkValues: map[string]any{}})
	case operator.OpEQ, operator.OpGT, operator.OpLT, operator.OpGTEQ, operator.OpLTEQ, operator.OpNEQ:
		col, isCol := pre.Left.(visitor.Column)
		right, isVals := pre.Right.(visitor.ValueExpr)
		if !isCol || !isVals {
			return sharding.EmptyResp, ErrUnsupportedTooComplexQuery
		}
		return b.algorithm.Sharding(ctx,
			sharding.Request{Op: pre.Op, SkValues: map[string]any{col.Name: right.Val}})
	default:
		return sharding.EmptyResp, NewUnsupportedOperatorError(pre.Op.Text)
	}
}

func (b *SelectHandler) negatePredicate(pre visitor.Predicate) (visitor.Predicate, error) {
	switch pre.Op {
	case operator.OpAnd:
		left, err := b.negatePredicate(pre.Left.(visitor.Predicate))
		if err != nil {
			return visitor.Predicate{}, err
		}
		right, err := b.negatePredicate(pre.Right.(visitor.Predicate))
		if err != nil {
			return visitor.Predicate{}, err
		}
		return visitor.Predicate{
			Left: left, Op: operator.OpOr, Right: right,
		}, nil
	case operator.OpOr:
		left, err := b.negatePredicate(pre.Left.(visitor.Predicate))
		if err != nil {
			return visitor.Predicate{}, err
		}
		right, err := b.negatePredicate(pre.Right.(visitor.Predicate))
		if err != nil {
			return visitor.Predicate{}, err
		}
		return visitor.Predicate{
			Left: left, Op: operator.OpOr, Right: right,
		}, nil
	default:
		nOp, err := operator.NegateOp(pre.Op)
		if err != nil {
			return visitor.Predicate{}, err
		}
		return visitor.Predicate{Left: pre.Left, Op: nOp, Right: pre.Right}, nil
	}
}

// mergeAnd 两个分片结果的交集
func (*SelectHandler) mergeAnd(left, right sharding.Response) sharding.Response {
	dsts := slice.IntersectSetFunc[sharding.Dst](left.Dsts, right.Dsts, func(src, dst sharding.Dst) bool {
		return src.Equals(dst)
	})
	return sharding.Response{Dsts: dsts}
}

// mergeOR 两个分片结果的并集
func (*SelectHandler) mergeOR(left, right sharding.Response) sharding.Response {
	dsts := slice.UnionSetFunc[sharding.Dst](left.Dsts, right.Dsts, func(src, dst sharding.Dst) bool {
		return src.Equals(dst)
	})
	return sharding.Response{Dsts: dsts}
}

// mergeIN 多个分片结果的并集
func (*SelectHandler) mergeIN(vals []sharding.Response) sharding.Response {
	var dsts []sharding.Dst
	for _, val := range vals {
		dsts = slice.UnionSetFunc[sharding.Dst](dsts, val.Dsts, func(src, dst sharding.Dst) bool {
			return src.Equals(dst)
		})
	}
	return sharding.Response{Dsts: dsts}
}

package sharding

import (
	"context"

	"github.com/ecodeclub/ekit/slice"
	"github.com/meoying/dbproxy/internal/protocol/mysql/plugin/visitor"
	"github.com/meoying/dbproxy/internal/sharding"
	"github.com/meoying/dbproxy/internal/sharding/operator"
)

type shardingBuilder struct {
	algorithm sharding.Algorithm
}

func (s *shardingBuilder) findDst(ctx context.Context, predicate visitor.Predicate) (sharding.Response, error) {
	if predicate != (visitor.Predicate{}) {
		return s.findDstByPredicate(ctx, predicate)
	}
	return sharding.Response{
		Dsts: s.algorithm.Broadcast(ctx),
	}, nil
}

func (b *shardingBuilder) findDstByPredicate(ctx context.Context, pre visitor.Predicate) (sharding.Response, error) {
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

func (b *shardingBuilder) negatePredicate(pre visitor.Predicate) (visitor.Predicate, error) {
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
func (*shardingBuilder) mergeAnd(left, right sharding.Response) sharding.Response {
	dsts := slice.IntersectSetFunc[sharding.Dst](left.Dsts, right.Dsts, func(src, dst sharding.Dst) bool {
		return src.Equals(dst)
	})
	return sharding.Response{Dsts: dsts}
}

// mergeOR 两个分片结果的并集
func (*shardingBuilder) mergeOR(left, right sharding.Response) sharding.Response {
	dsts := slice.UnionSetFunc[sharding.Dst](left.Dsts, right.Dsts, func(src, dst sharding.Dst) bool {
		return src.Equals(dst)
	})
	return sharding.Response{Dsts: dsts}
}

// mergeIN 多个分片结果的并集
func (*shardingBuilder) mergeIN(vals []sharding.Response) sharding.Response {
	var dsts []sharding.Dst
	for _, val := range vals {
		dsts = slice.UnionSetFunc[sharding.Dst](dsts, val.Dsts, func(src, dst sharding.Dst) bool {
			return src.Equals(dst)
		})
	}
	return sharding.Response{Dsts: dsts}
}

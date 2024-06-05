package sharding

import (
	"context"
	"database/sql"
	"errors"
	"github.com/meoying/dbproxy/internal/datasource"
	"github.com/meoying/dbproxy/internal/protocol/mysql/plugin/visitor"
	"github.com/meoying/dbproxy/internal/sharding"
	"github.com/valyala/bytebufferpool"
	"go.uber.org/multierr"
	"sync"
)

var NewErrUnsupportedExpressionType = errors.New("不支持 Expression")

type builder struct {
	buffer *bytebufferpool.ByteBuffer
	args   []any
}

func (b *builder) quote(val string) {
	b.writeByte('`')
	b.writeString(val)
	b.writeByte('`')
}

func (b *builder) space() {
	b.writeByte(' ')
}

func (b *builder) point() {
	b.writeByte('.')
}

func (b *builder) writeString(val string) {
	_, _ = b.buffer.WriteString(val)
}

func (b *builder) writeByte(c byte) {
	_ = b.buffer.WriteByte(c)
}

func (b *builder) end() {
	b.writeByte(';')
}

func (b *builder) comma() {
	b.writeByte(',')
}

func (b *builder) parameter(arg interface{}) {
	if b.args == nil {
		b.args = make([]interface{}, 0, 4)
	}
	b.writeByte('?')
	b.args = append(b.args, arg)
}

func (b *builder) addArgs(args ...any) {
	if b.args == nil {
		b.args = make([]any, 0, 8)
	}
	b.args = append(b.args, args...)
}

func (b *builder) buildColumn(c visitor.Column) error {
	b.quote(c.Name)
	if c.Alias != "" {
		// b.aliases[c.alias] = struct{}{}
		b.writeString(" AS ")
		b.quote(c.Alias)
	}
	return nil
}
func (b *builder) buildRawExpr(e visitor.RawExpr) {
	b.writeString(e.Raw)
	b.args = append(b.args, e.Args...)
}
func (b *builder) buildBinaryExpr(e visitor.BinaryExpr) error {
	err := b.buildSubExpr(e.Left)
	if err != nil {
		return err
	}
	b.writeString(e.Op.Text)
	return b.buildSubExpr(e.Right)
}

func (b *builder) buildSubExpr(subExpr visitor.Expr) error {
	switch r := subExpr.(type) {
	case visitor.Predicate:
		b.writeByte('(')
		if err := b.buildBinaryExpr(visitor.BinaryExpr(r)); err != nil {
			return err
		}
		b.writeByte(')')
	default:
		if err := b.buildExpr(r); err != nil {
			return err
		}
	}
	return nil
}

func (b *builder) buildExpr(expr visitor.Expr) error {
	switch e := expr.(type) {
	case nil:
	case visitor.RawExpr:
		b.buildRawExpr(e)
	case visitor.Column:
		// _, ok := b.aliases[e.name]
		// if ok {
		// 	b.quote(e.name)
		// 	return nil
		// }
		return b.buildColumn(e)
	case visitor.ValueExpr:
		b.parameter(e.Val)
	case visitor.BinaryExpr:
		if err := b.buildBinaryExpr(e); err != nil {
			return err
		}
	case visitor.Predicate:
		if err := b.buildBinaryExpr(visitor.BinaryExpr(e)); err != nil {
			return err
		}
	case visitor.Values:
		if err := b.buildIns(e); err != nil {
			return err
		}
	default:
		return NewErrUnsupportedExpressionType
	}
	return nil
}

func (b *builder) buildIns(is visitor.Values) error {
	b.writeByte('(')
	for idx, inVal := range is.Vals {
		if idx > 0 {
			b.writeByte(',')
		}

		b.args = append(b.args, inVal)
		b.writeByte('?')

	}
	b.writeByte(')')
	return nil
}

func exec(ctx context.Context, db datasource.DataSource,qs []sharding.Query)sharding.Result  {
	errList := make([]error, len(qs))
	resList := make([]sql.Result, len(qs))
	var wg sync.WaitGroup
	locker := &sync.RWMutex{}
	wg.Add(len(qs))
	for idx, q := range qs {
		go func(idx int, q sharding.Query) {
			defer wg.Done()
			res, er := db.Exec(ctx, q)
			locker.Lock()
			errList[idx] = er
			resList[idx] = res
			locker.Unlock()
		}(idx, q)
	}
	wg.Wait()
	shardingRes := sharding.NewResult(resList, multierr.Combine(errList...))
	return shardingRes
}
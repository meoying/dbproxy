package statement

import (
	"context"
	"github.com/meoying/dbproxy/internal/datasource"
	"github.com/meoying/dbproxy/internal/datasource/internal/errs"
)

const (
	Delay = "delay"
)

type StmtFactory interface {
	StmtOf(ctx Context, finder datasource.Finder) (datasource.Stmt, error)
}

type Context struct {
	StmtName string
	StmtCtx  context.Context
}

type stmtTypeKey struct{}

func UsingStmtType(ctx context.Context, val string) context.Context {
	return context.WithValue(ctx, stmtTypeKey{}, val)
}

func GetCtxTypeKey(ctx context.Context) any {
	return ctx.Value(stmtTypeKey{})
}

type StmtFacade struct {
	factory StmtFactory
	finder  datasource.Finder
}

func NewStmtFacade(ctx context.Context, finder datasource.Finder) (StmtFacade, error) {
	res := StmtFacade{
		finder: finder,
	}
	switch GetCtxTypeKey(ctx).(string) {
	case Delay:
		res.factory = DelayStmtFactory{}
		return res, nil
	default:
		return StmtFacade{}, errs.ErrUnsupportedDistributedPrepare
	}
}

func (s *StmtFacade) Prepare(ctx context.Context) (datasource.Stmt, error) {
	dsCtx := Context{
		StmtCtx:  ctx,
		StmtName: GetCtxTypeKey(ctx).(string),
	}
	return s.factory.StmtOf(dsCtx, s.finder)
}

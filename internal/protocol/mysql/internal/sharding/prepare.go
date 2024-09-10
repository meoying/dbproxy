package sharding

import (
	"context"
	"database/sql"
	"go.uber.org/multierr"
	"golang.org/x/sync/errgroup"
	"sync"

	"github.com/ecodeclub/ekit/list"
	"github.com/ecodeclub/ekit/sqlx"
	"github.com/meoying/dbproxy/internal/datasource"
	"github.com/meoying/dbproxy/internal/merger/factory"
	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/pcontext"
	vbuilder "github.com/meoying/dbproxy/internal/protocol/mysql/internal/visitor/builder"
	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/visitor/vparser"
	"github.com/meoying/dbproxy/internal/sharding"
)

type PrepareHandler struct {
	shardingBuilder
	prepareVal vparser.PrepareVal
	stmt       datasource.Stmt
	args       []any

	algorithm  sharding.Algorithm
	db         datasource.DataSource
	prepareCtx *pcontext.Context

	stmtHandlers map[string]NewHandlerFunc
	stmtHandler  ShardingHandler
}

func (p *PrepareHandler) Build(ctx context.Context) ([]sharding.Query, error) {
	sqlTypeName := p.prepareCtx.ParsedQuery.Type()
	newStmtHandler, ok := p.stmtHandlers[sqlTypeName]
	if !ok {
		return nil, ErrUnKnowSql
	}

	query, err := p.ReplacePlaceholder()
	if err != nil {
		return nil, err
	}
	pctx := &pcontext.Context{
		Context:     ctx,
		Query:       query,
		ParsedQuery: pcontext.NewParsedQuery(query, vparser.NewHintVisitor()),
	}
	stmtHandler, err := newStmtHandler(p.algorithm, p.db, pctx)
	if err != nil {
		return nil, err
	}

	// 获取分库分表后的sql
	qs, err := stmtHandler.Build(ctx)
	if err != nil {
		return nil, err
	}

	// 转成对应prepare语句
	qs4prepare := make([]sharding.Query, 0, len(qs))
	for _, q := range qs {
		prepareBuilder := vbuilder.NewPrepare(q.DB, q.Table, p.prepareCtx.ParsedQuery)
		parsedQuery := pcontext.NewParsedQuery(q.SQL, vparser.NewHintVisitor())
		que, args, err := prepareBuilder.BuildPrepareQuery(parsedQuery.Root())
		if err != nil {
			return nil, err
		}
		qs4prepare = append(qs4prepare, sharding.Query{
			SQL:        que,
			DB:         q.DB,
			Table:      q.Table,
			Datasource: q.Datasource,
			Args:       args,
		})
	}

	p.stmtHandler = stmtHandler

	return qs4prepare, nil
}

func (p *PrepareHandler) ReplacePlaceholder() (string, error) {
	prepareBuilder := vbuilder.NewPrepare("", "", p.prepareCtx.ParsedQuery)
	parsedQuery := pcontext.NewParsedQuery(p.prepareCtx.Query, vparser.NewHintVisitor())
	que, err := prepareBuilder.ReplacePlaceholders(parsedQuery.Root(), p.args)
	if err != nil {
		return "", err
	}
	return que, nil
}

func (p *PrepareHandler) QueryOrExec(ctx context.Context) (*Result, error) {
	qs, err := p.Build(p.prepareCtx)
	if err != nil {
		return nil, err
	}

	var rows sqlx.Rows
	var res sql.Result
	switch p.prepareCtx.ParsedQuery.Type() {
	case vparser.SelectStmt:
		handler := p.stmtHandler.(*SelectHandler)
		originCols, targetCols, err := handler.NewQuerySpec()
		if err != nil {
			return nil, err
		}
		mgr, err := factory.New(originCols, targetCols)
		if err != nil {
			return nil, err
		}
		rowsList, err := p.queryMulti(ctx, qs)
		if err != nil {
			return nil, err
		}
		rows, err = mgr.Merge(ctx, rowsList.AsSlice())
		if err != nil {
			return nil, err
		}
	case vparser.InsertStmt, vparser.UpdateStmt, vparser.DeleteStmt:
		res = p.execMulti(ctx, qs)
	}
	return &Result{
		Rows:   rows,
		Result: res,
	}, nil
}

func NewPrepareHandler(stmt datasource.Stmt, a sharding.Algorithm, db datasource.DataSource, prepareCtx *pcontext.Context, args []any) (*PrepareHandler, error) {
	prepareVisitor := vparser.NewPrepareVisitor()
	resp := prepareVisitor.Parse(prepareCtx.ParsedQuery.Root())
	baseVal := resp.(vparser.BaseVal)
	if baseVal.Err != nil {
		return nil, baseVal.Err
	}
	prepareVal := baseVal.Data.(vparser.PrepareVal)
	if prepareVal.PlaceHolderCount != len(args) {
		return nil, ErrPrepareArgsNoEqual
	}
	return &PrepareHandler{
		prepareVal: prepareVal,
		stmt:       stmt,
		algorithm:  a,
		db:         db,
		prepareCtx: prepareCtx,
		args:       args,
		stmtHandlers: map[string]NewHandlerFunc{
			vparser.SelectStmt: NewSelectHandler,
			vparser.InsertStmt: NewInsertBuilder,
			vparser.UpdateStmt: NewUpdateHandler,
			vparser.DeleteStmt: NewDeleteHandler,
		},
		shardingBuilder: shardingBuilder{
			algorithm: a,
		},
	}, nil
}

func (p *PrepareHandler) queryMulti(ctx context.Context, qs []sharding.Query) (list.List[sqlx.Rows], error) {
	res := &list.ConcurrentList[sqlx.Rows]{
		List: list.NewArrayList[sqlx.Rows](len(qs)),
	}
	var eg errgroup.Group
	for _, q := range qs {
		eg.Go(func() error {
			rs, err := p.stmt.Query(ctx, q)
			if err == nil {
				return res.Append(rs)
			}
			return err
		})
	}
	return res, eg.Wait()
}

func (p *PrepareHandler) execMulti(ctx context.Context, qs []sharding.Query) sharding.Result {
	errList := make([]error, len(qs))
	resList := make([]sql.Result, len(qs))
	var wg sync.WaitGroup
	locker := &sync.RWMutex{}
	wg.Add(len(qs))
	for idx, q := range qs {
		go func(idx int, q sharding.Query) {
			defer wg.Done()
			res, er := p.stmt.Exec(ctx, q)
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

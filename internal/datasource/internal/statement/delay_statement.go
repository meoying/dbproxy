package statement

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/meoying/dbproxy/internal/datasource"
	"go.uber.org/multierr"
	"sync"
)

var _ datasource.Stmt = &DelayStmt{}

type DelayStmt struct {
	lock   sync.RWMutex
	stmts  map[string]datasource.Stmt
	finder datasource.Finder
}

func (s *DelayStmt) findTgt(ctx context.Context, query datasource.Query) (datasource.DataSource, error) {
	return s.finder.FindTgt(ctx, query)
}

func (s *DelayStmt) findOrPrepare(ctx context.Context, query datasource.Query) (datasource.Stmt, error) {
	key := query.Datasource + "." + query.DB + "." + query.Table

	s.lock.RLock()
	stmt, ok := s.stmts[key]
	s.lock.RUnlock()
	if ok {
		return stmt, nil
	}
	s.lock.Lock()
	defer s.lock.Unlock()
	if stmt, ok = s.stmts[key]; ok {
		return stmt, nil
	}
	ds, err := s.findTgt(ctx, query)
	if err != nil {
		return nil, err
	}
	stmt, err = ds.Prepare(ctx, query)
	if err != nil {
		return nil, err
	}
	s.stmts[key] = stmt
	return stmt, nil
}

func (s *DelayStmt) Query(ctx context.Context, query datasource.Query) (*sql.Rows, error) {
	stmt, err := s.findOrPrepare(ctx, query)
	if err != nil {
		return nil, err
	}
	return stmt.Query(ctx, query)
}

func (s *DelayStmt) Exec(ctx context.Context, query datasource.Query) (sql.Result, error) {
	stmt, err := s.findOrPrepare(ctx, query)
	if err != nil {
		return nil, err
	}
	return stmt.Exec(ctx, query)
}

func (s *DelayStmt) Close() error {
	var err error
	for name, stmt := range s.stmts {
		if er := stmt.Close(); er != nil {
			err = multierr.Combine(
				err, fmt.Errorf("masterslave DB name [%s] Prepare Close error: %w", name, er))
		}
	}
	return err
}

func NewDelayStmt(finder datasource.Finder) *DelayStmt {
	return &DelayStmt{
		stmts:  make(map[string]datasource.Stmt, 8),
		finder: finder,
	}
}

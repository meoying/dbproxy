package statement

import (
	"context"
	"database/sql"

	"github.com/meoying/dbproxy/internal/datasource"
)

var (
	_ datasource.Stmt = &PreparedStatement{}
)

type PreparedStatement struct {
	stmt *sql.Stmt
}

func NewPreparedStatement(stmt *sql.Stmt) *PreparedStatement {
	return &PreparedStatement{
		stmt: stmt,
	}
}

func (p *PreparedStatement) Query(ctx context.Context, query datasource.Query) (*sql.Rows, error) {
	return p.stmt.QueryContext(ctx, query.Args...)
}

func (p *PreparedStatement) Exec(ctx context.Context, query datasource.Query) (sql.Result, error) {
	return p.stmt.ExecContext(ctx, query.Args...)
}

func (p *PreparedStatement) Close() error {
	return p.stmt.Close()
}

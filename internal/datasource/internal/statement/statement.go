package statement

import (
	"context"
	"database/sql"
	"log"

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
	log.Printf("PreparedStatement ds: QueryContext 执行前query = %#v\n", query)
	rows, err := p.stmt.QueryContext(ctx, query.Args...)
	log.Printf("PreparedStatement ds: QueryContext 执行后 QueryContext query = %#v, rows = %#v, err = %#v, \n", query, rows, err)
	return rows, err
}

func (p *PreparedStatement) Exec(ctx context.Context, query datasource.Query) (sql.Result, error) {
	return p.stmt.ExecContext(ctx, query.Args...)
}

func (p *PreparedStatement) Close() error {
	return p.stmt.Close()
}

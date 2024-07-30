package sharding

import (
	"context"
	"database/sql/driver"

	"github.com/ecodeclub/ekit/slice"
	"github.com/meoying/dbproxy/internal/datasource"
)

type stmt struct {
	conn  *connection
	stmt  datasource.Stmt
	query string
}

func (s *stmt) Exec(args []driver.Value) (driver.Result, error) {
	return s.ExecContext(context.Background(), slice.Map(args, func(idx int, src driver.Value) driver.NamedValue {
		return driver.NamedValue{Value: src}
	}))
}

func (s *stmt) Query(args []driver.Value) (driver.Rows, error) {
	return s.QueryContext(context.Background(), slice.Map(args, func(idx int, src driver.Value) driver.NamedValue {
		return driver.NamedValue{Value: src}
	}))
}

func (s *stmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	return s.stmt.Exec(ctx, datasource.Query{
		SQL: s.query,
		Args: slice.Map(args, func(idx int, src driver.NamedValue) any {
			return src
		}),
		DB:         "stmt.ExecContext中DB",
		Datasource: "stmt.ExecContext中Datasource",
	})
}

func (s *stmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	r, err := s.stmt.Query(ctx, datasource.Query{
		SQL: s.query,
		Args: slice.Map(args, func(idx int, src driver.NamedValue) any {
			return src
		}),
		DB:         "stmt.QueryContext中DB",
		Datasource: "stmt.QueryContext中Datasource",
	})
	return &rows{sqlxRows: r}, err
}

func (s *stmt) NumInput() int {
	return -1
}

func (s *stmt) CheckNamedValue(value *driver.NamedValue) error {
	return nil
}

func (s *stmt) ColumnConverter(idx int) driver.ValueConverter {
	// TODO implement me
	panic("implement me")
}

func (s *stmt) Close() error {
	return s.stmt.Close()
}

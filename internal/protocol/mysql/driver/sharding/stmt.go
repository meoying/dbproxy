package sharding

import (
	"context"
	"database/sql/driver"
)

type stmt struct {
}

func (s *stmt) Close() error {
	// TODO implement me
	panic("implement me")
}

func (s *stmt) NumInput() int {
	// TODO implement me
	panic("implement me")
}

func (s *stmt) Exec(args []driver.Value) (driver.Result, error) {
	// TODO implement me
	panic("implement me")
}

func (s *stmt) Query(args []driver.Value) (driver.Rows, error) {
	// TODO implement me
	panic("implement me")
}

func (s *stmt) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	// TODO implement me
	panic("implement me")
}

func (s *stmt) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	// TODO implement me
	panic("implement me")
}

func (s *stmt) CheckNamedValue(value *driver.NamedValue) error {
	// TODO implement me
	panic("implement me")
}

func (s *stmt) ColumnConverter(idx int) driver.ValueConverter {
	// TODO implement me
	panic("implement me")
}

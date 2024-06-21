package log

import (
	"context"
	"database/sql/driver"
)

type stmtWrapper struct {
	stmt   driver.Stmt
	logger Logger
}

func (s *stmtWrapper) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	// TODO implement me
	panic("implement me")
}

func (s *stmtWrapper) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	// TODO implement me
	panic("implement me")
}

func (s *stmtWrapper) CheckNamedValue(value *driver.NamedValue) error {
	// TODO implement me
	panic("implement me")
}

func (s *stmtWrapper) ColumnConverter(idx int) driver.ValueConverter {
	// TODO implement me
	panic("implement me")
}

func (s *stmtWrapper) Close() error {
	s.logger.Logf("Close statement")
	err := s.stmt.Close()
	if err != nil {
		s.logger.Errorf("Failed to Close statement: %v", err)
		return err
	}
	return nil
}

func (s *stmtWrapper) NumInput() int {
	return s.stmt.NumInput()
}

func (s *stmtWrapper) Exec(args []driver.Value) (driver.Result, error) {
	result, err := s.stmt.Exec(args)
	if err != nil {
		s.logger.Errorf("Failed to Execute statement: %v", err)
		return nil, err
	}
	s.logger.Logf("Execute statement with args: %v", args)
	return &resultWrapper{result: result, logger: s.logger}, nil
}

func (s *stmtWrapper) Query(args []driver.Value) (driver.Rows, error) {
	s.logger.Logf("Query statement with args: %v", args)
	rows, err := s.stmt.Query(args)
	if err != nil {
		s.logger.Errorf("Failed to Query statement: %v", err)
		return nil, err
	}
	return newRowsWrapper(rows, s.logger), nil
}

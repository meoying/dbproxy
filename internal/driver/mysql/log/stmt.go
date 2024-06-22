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
	result, err := s.stmt.(driver.StmtExecContext).ExecContext(ctx, args)
	if err != nil {
		s.logger.Errorf("Failed to Exec statement: %v", err)
		return nil, err
	}
	s.logger.Logf("Exec statement with args: %v", args)
	return &resultWrapper{result: result, logger: s.logger}, nil
}

func (s *stmtWrapper) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	rows, err := s.stmt.(driver.StmtQueryContext).QueryContext(ctx, args)
	if err != nil {
		s.logger.Errorf("Failed to Query statement: %v", err)
		return nil, err
	}
	s.logger.Logf("Query statement with args: %v", args)
	return &rowsWrapper{rows: rows, logger: s.logger}, nil
}

func (s *stmtWrapper) CheckNamedValue(value *driver.NamedValue) error {
	err := s.stmt.(driver.NamedValueChecker).CheckNamedValue(value)
	if err != nil {
		s.logger.Errorf("Failed to CheckNamedValue statement: %v", err)
		return err
	}
	s.logger.Logf("CheckNamedValue statement with args: %v", value)
	return nil
}

func (s *stmtWrapper) ColumnConverter(idx int) driver.ValueConverter {
	s.logger.Logf("Column converter with idx: %v", idx)
	return s.stmt.(driver.ColumnConverter).ColumnConverter(idx)
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
	rows, err := s.stmt.Query(args)
	if err != nil {
		s.logger.Errorf("Failed to Query statement: %v", err)
		return nil, err
	}
	s.logger.Logf("Query statement with args: %v", args)
	return &rowsWrapper{rows: rows, logger: s.logger}, nil
}

func (s *stmtWrapper) NumInput() int {
	i := s.stmt.NumInput()
	s.logger.Logf("NumInput: %d", i)
	return i
}

func (s *stmtWrapper) Close() error {
	err := s.stmt.Close()
	if err != nil {
		s.logger.Errorf("Failed to Close statement: %v", err)
		return err
	}
	s.logger.Logf("Close statement")
	return nil
}

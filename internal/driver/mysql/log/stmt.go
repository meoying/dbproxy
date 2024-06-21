package log

import (
	"context"
	"database/sql/driver"
)

type stmtWrapper struct {
	stmt   driver.Stmt
	logger Logger
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

type stmtQueryContextWrapper struct {
	driver.StmtQueryContext
	logger Logger
}

func (s *stmtQueryContextWrapper) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	rows, err := s.StmtQueryContext.QueryContext(ctx, args)
	if err != nil {
		s.logger.Errorf("Failed to Query statement: %v", err)
		return nil, err
	}
	s.logger.Logf("Query statement with args: %v", args)
	return &rowsWrapper{rows: rows, logger: s.logger}, nil
}

type stmtExecContextWrapper struct {
	driver.StmtExecContext
	logger Logger
}

func (s *stmtExecContextWrapper) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	result, err := s.StmtExecContext.ExecContext(ctx, args)
	if err != nil {
		s.logger.Errorf("Failed to Exec statement: %v", err)
		return nil, err
	}
	s.logger.Logf("Exec statement with args: %v", args)
	return &resultWrapper{result: result, logger: s.logger}, nil
}

type namedValueCheckerWrapper struct {
	driver.NamedValueChecker
	logger Logger
}

func (n *namedValueCheckerWrapper) CheckNamedValue(value *driver.NamedValue) error {
	err := n.NamedValueChecker.CheckNamedValue(value)
	if err != nil {
		n.logger.Errorf("Failed to CheckNamedValue statement: %v", err)
		return err
	}
	n.logger.Logf("CheckNamedValue statement with args: %v", value)
	return nil
}

type columnConverterWrapper struct {
	columnConverter driver.ColumnConverter
	logger          Logger
}

func (c *columnConverterWrapper) ColumnConverter(idx int) driver.ValueConverter {
	c.logger.Logf("Column converter with idx: %v", idx)
	return c.columnConverter.ColumnConverter(idx)
}

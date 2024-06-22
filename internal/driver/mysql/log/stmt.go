package log

import (
	"context"
	"database/sql/driver"
)

type stmtWrapper struct {
	stmt   driver.Stmt
	logger logger
}

func (s *stmtWrapper) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	result, err := s.stmt.(driver.StmtExecContext).ExecContext(ctx, args)
	if err != nil {
		s.logger.Error("failed to execute statement", "error", err, "args", args)
		return nil, err
	}
	s.logger.Info("executed statement", "args", args)
	return &resultWrapper{result: result, logger: s.logger}, nil
}

func (s *stmtWrapper) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	rows, err := s.stmt.(driver.StmtQueryContext).QueryContext(ctx, args)
	if err != nil {
		s.logger.Error("failed to query statement", "error", err, "args", args)
		return nil, err
	}
	s.logger.Info("queried statement", "args", args)
	return &rowsWrapper{rows: rows, logger: s.logger}, nil
}

func (s *stmtWrapper) CheckNamedValue(value *driver.NamedValue) error {
	err := s.stmt.(driver.NamedValueChecker).CheckNamedValue(value)
	if err != nil {
		s.logger.Error("failed to check named value", "error", err, "value", value)
		return err
	}
	s.logger.Info("checked named value", "value", value)
	return nil
}

func (s *stmtWrapper) ColumnConverter(idx int) driver.ValueConverter {
	converter := s.stmt.(driver.ColumnConverter).ColumnConverter(idx)
	s.logger.Info("retrieved column converter", "index", idx)
	return converter
}

func (s *stmtWrapper) Exec(args []driver.Value) (driver.Result, error) {
	result, err := s.stmt.Exec(args)
	if err != nil {
		s.logger.Error("failed to execute statement", "error", err, "args", args)
		return nil, err
	}
	s.logger.Info("executed statement", "args", args)
	return &resultWrapper{result: result, logger: s.logger}, nil
}

func (s *stmtWrapper) Query(args []driver.Value) (driver.Rows, error) {
	rows, err := s.stmt.Query(args)
	if err != nil {
		s.logger.Error("failed to query statement", "error", err, "args", args)
		return nil, err
	}
	s.logger.Info("queried statement", "args", args)
	return &rowsWrapper{rows: rows, logger: s.logger}, nil
}

func (s *stmtWrapper) NumInput() int {
	count := s.stmt.NumInput()
	s.logger.Info("retrieved number of inputs", "count", count)
	return count
}

func (s *stmtWrapper) Close() error {
	err := s.stmt.Close()
	if err != nil {
		s.logger.Error("failed to close statement", "error", err)
		return err
	}
	s.logger.Info("closed statement")
	return nil
}

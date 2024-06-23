package log

import (
	"context"
	"database/sql/driver"

	"github.com/ecodeclub/ekit/slice"
)

type stmtWrapper struct {
	stmt   driver.Stmt
	query  string
	logger logger
}

func (s *stmtWrapper) ExecContext(ctx context.Context, args []driver.NamedValue) (driver.Result, error) {
	result, err := s.stmt.(driver.StmtExecContext).ExecContext(ctx, args)
	if err != nil {
		s.logger.Error("执行语句失败", "错误", err, "语句", s.query, "参数", args)
		return nil, err
	}
	s.logger.Info("执行语句成功", "语句", s.query, "参数", args)
	return &resultWrapper{result: result, logger: s.logger}, nil
}

func (s *stmtWrapper) QueryContext(ctx context.Context, args []driver.NamedValue) (driver.Rows, error) {
	rows, err := s.stmt.(driver.StmtQueryContext).QueryContext(ctx, args)
	if err != nil {
		s.logger.Error("查询语句失败", "错误", err, "语句", s.query, "参数", args)
		return nil, err
	}
	s.logger.Info("查询语句成功", "语句", s.query, "参数", args)
	return &rowsWrapper{rows: rows, logger: s.logger}, nil
}

func (s *stmtWrapper) CheckNamedValue(value *driver.NamedValue) error {
	err := s.stmt.(driver.NamedValueChecker).CheckNamedValue(value)
	if err != nil {
		s.logger.Error("检查命名值失败", "错误", err, "值", value)
		return err
	}
	s.logger.Info("检查命名值成功", "值", value)
	return nil
}

func (s *stmtWrapper) ColumnConverter(idx int) driver.ValueConverter {
	converter := s.stmt.(driver.ColumnConverter).ColumnConverter(idx)
	s.logger.Info("获取列转换器", "索引", idx)
	return converter
}

func (s *stmtWrapper) Exec(args []driver.Value) (driver.Result, error) {
	return s.ExecContext(context.Background(), slice.Map(args, func(idx int, src driver.Value) driver.NamedValue {
		return driver.NamedValue{
			Value: src,
		}
	}))
}

func (s *stmtWrapper) Query(args []driver.Value) (driver.Rows, error) {
	return s.QueryContext(context.Background(), slice.Map(args, func(idx int, src driver.Value) driver.NamedValue {
		return driver.NamedValue{
			Value: src,
		}
	}))
}

func (s *stmtWrapper) NumInput() int {
	count := s.stmt.NumInput()
	s.logger.Info("获取占位符数量", "语句", s.query, "数量", count)
	return count
}

func (s *stmtWrapper) Close() error {
	err := s.stmt.Close()
	if err != nil {
		s.logger.Error("关闭语句失败", "语句", s.query, "错误", err)
		return err
	}
	s.logger.Info("关闭语句成功", "语句", s.query)
	return nil
}

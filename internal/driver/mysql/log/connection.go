package log

import (
	"context"
	"database/sql/driver"

	driver2 "github.com/meoying/dbproxy/internal/driver"
)

var _ driver2.Conn = &connWrapper{}

type connWrapper struct {
	conn   driver.Conn
	logger logger
}

func (c *connWrapper) Ping(ctx context.Context) error {
	err := c.conn.(driver.Pinger).Ping(ctx)
	if err != nil {
		c.logger.Error("连接连通性检查失败", "错误", err)
		return err
	}
	c.logger.Info("连接连通性检查成功")
	return nil
}

func (c *connWrapper) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	result, err := c.conn.(driver.ExecerContext).ExecContext(ctx, query, args)
	if err != nil {
		c.logger.Error("执行失败", "查询", query, "错误", err)
		return nil, err
	}
	c.logger.Info("执行成功", "查询", query, "参数", args)
	return result, nil
}

func (c *connWrapper) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	rows, err := c.conn.(driver.QueryerContext).QueryContext(ctx, query, args)
	if err != nil {
		c.logger.Error("查询失败", "查询", query, "错误", err)
		return nil, err
	}
	c.logger.Info("查询成功", "查询", query, "参数", args)
	return &rowsWrapper{rows: rows, logger: c.logger}, nil
}

func (c *connWrapper) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	stmt, err := c.conn.(driver.ConnPrepareContext).PrepareContext(ctx, query)
	if err != nil {
		c.logger.Error("预编译失败", "查询", query, "错误", err)
		return nil, err
	}
	c.logger.Info("预编译成功", "查询", query)
	return &stmtWrapper{stmt: stmt, query: query, logger: c.logger}, nil
}

func (c *connWrapper) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	tx, err := c.conn.(driver.ConnBeginTx).BeginTx(ctx, opts)
	if err != nil {
		c.logger.Error("开始事务失败", "选项", opts, "错误", err)
		return nil, err
	}
	c.logger.Info("开始事务成功", "选项", opts)
	return &txWrapper{tx: tx, logger: c.logger}, nil
}

func (c *connWrapper) ResetSession(ctx context.Context) error {
	err := c.conn.(driver.SessionResetter).ResetSession(ctx)
	if err != nil {
		c.logger.Error("重置会话失败", "错误", err)
		return err
	}
	c.logger.Info("重置会话成功")
	return nil
}

func (c *connWrapper) IsValid() bool {
	valid := c.conn.(driver.Validator).IsValid()
	c.logger.Info("连接是否有效", valid)
	return valid
}

func (c *connWrapper) CheckNamedValue(value *driver.NamedValue) error {
	err := c.conn.(driver.NamedValueChecker).CheckNamedValue(value)
	if err != nil {
		c.logger.Error("检查命名值失败", "值", value, "错误", err)
		return err
	}
	c.logger.Info("检查命名值成功", "值", value)
	return nil
}

func (c *connWrapper) Prepare(query string) (driver.Stmt, error) {
	return c.PrepareContext(context.Background(), query)
}

func (c *connWrapper) Close() error {
	err := c.conn.Close()
	if err != nil {
		c.logger.Error("关闭连接失败", "错误", err)
		return err
	}
	c.logger.Info("连接关闭成功")
	return nil
}

// Begin starts and returns a new transaction.
//
// Deprecated: Drivers should implement ConnBeginTx instead (or additionally).
func (c *connWrapper) Begin() (driver.Tx, error) {
	return c.BeginTx(context.Background(), driver.TxOptions{})
}

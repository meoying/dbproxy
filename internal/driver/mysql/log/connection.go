package log

import (
	"context"
	"database/sql/driver"
)

type connWrapper struct {
	conn   driver.Conn
	logger logger
}

func (c *connWrapper) Ping(ctx context.Context) error {
	err := c.conn.(driver.Pinger).Ping(ctx)
	if err != nil {
		c.logger.Error("ping failed", "error", err)
		return err
	}
	c.logger.Info("ping successful")
	return nil
}

func (c *connWrapper) Exec(query string, args []driver.Value) (driver.Result, error) {
	r, err := c.conn.(driver.Execer).Exec(query, args)
	if err != nil {
		c.logger.Error("exec query failed", "query", query, "error", err)
		return nil, err
	}
	c.logger.Info("exec query successful", "query", query, "args", args)
	return r, nil
}

func (c *connWrapper) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	result, err := c.conn.(driver.ExecerContext).ExecContext(ctx, query, args)
	if err != nil {
		c.logger.Error("exec context failed", "query", query, "error", err)
		return nil, err
	}
	c.logger.Info("exec context successful", "query", query, "args", args)
	return result, nil
}

func (c *connWrapper) Query(query string, args []driver.Value) (driver.Rows, error) {
	rows, err := c.conn.(driver.Queryer).Query(query, args)
	if err != nil {
		c.logger.Error("query failed", "query", query, "error", err)
		return nil, err
	}
	c.logger.Info("query successful", "query", query, "args", args)
	return &rowsWrapper{rows: rows, logger: c.logger}, nil
}

func (c *connWrapper) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	rows, err := c.conn.(driver.QueryerContext).QueryContext(ctx, query, args)
	if err != nil {
		c.logger.Error("query context failed", "query", query, "error", err)
		return nil, err
	}
	c.logger.Info("query context successful", "query", query, "args", args)
	return &rowsWrapper{rows: rows, logger: c.logger}, nil
}

func (c *connWrapper) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	stmt, err := c.conn.(driver.ConnPrepareContext).PrepareContext(ctx, query)
	if err != nil {
		c.logger.Error("prepare context failed", "query", query, "error", err)
		return nil, err
	}
	c.logger.Info("prepare context successful", "query", query)
	return &stmtWrapper{stmt: stmt, logger: c.logger}, nil
}

func (c *connWrapper) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	tx, err := c.conn.(driver.ConnBeginTx).BeginTx(ctx, opts)
	if err != nil {
		c.logger.Error("begin transaction failed", "options", opts, "error", err)
		return nil, err
	}
	c.logger.Info("begin transaction successful", "options", opts)
	return &txWrapper{tx: tx, logger: c.logger}, nil
}

func (c *connWrapper) ResetSession(ctx context.Context) error {
	err := c.conn.(driver.SessionResetter).ResetSession(ctx)
	if err != nil {
		c.logger.Error("reset session failed", "error", err)
		return err
	}
	c.logger.Info("reset session successful")
	return nil
}

func (c *connWrapper) IsValid() bool {
	valid := c.conn.(driver.Validator).IsValid()
	c.logger.Info("connection validity checked", "valid", valid)
	return valid
}

func (c *connWrapper) CheckNamedValue(value *driver.NamedValue) error {
	err := c.conn.(driver.NamedValueChecker).CheckNamedValue(value)
	if err != nil {
		c.logger.Error("check named value failed", "value", value, "error", err)
		return err
	}
	c.logger.Info("check named value successful", "value", value)
	return nil
}

func (c *connWrapper) Prepare(query string) (driver.Stmt, error) {
	stmt, err := c.conn.Prepare(query)
	if err != nil {
		c.logger.Error("prepare query failed", "query", query, "error", err)
		return nil, err
	}
	c.logger.Info("prepare query successful", "query", query)
	return &stmtWrapper{stmt: stmt, logger: c.logger}, nil
}

func (c *connWrapper) Close() error {
	err := c.conn.Close()
	if err != nil {
		c.logger.Error("close connection failed", "error", err)
		return err
	}
	c.logger.Info("connection closed successfully")
	return nil
}

// Begin starts and returns a new transaction.
//
// Deprecated: Drivers should implement ConnBeginTx instead (or additionally).
func (c *connWrapper) Begin() (driver.Tx, error) {
	tx, err := c.conn.Begin()
	if err != nil {
		c.logger.Error("begin transaction failed", "error", err)
		return nil, err
	}
	c.logger.Info("transaction begun successfully")
	return &txWrapper{tx: tx, logger: c.logger}, nil
}

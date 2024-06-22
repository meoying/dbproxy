package log

import (
	"context"
	"database/sql/driver"
)

type connWrapper struct {
	conn   driver.Conn
	logger Logger
}

func (c *connWrapper) Ping(ctx context.Context) error {
	err := c.conn.(driver.Pinger).Ping(ctx)
	if err != nil {
		c.logger.Errorf("failed to ping: %v", err)
		return err
	}
	c.logger.Logf("successfully ping")
	return nil
}

func (c *connWrapper) Exec(query string, args []driver.Value) (driver.Result, error) {
	r, err := c.conn.(driver.Execer).Exec(query, args)
	if err != nil {
		c.logger.Errorf("Failed to execute query %s: %v", query, err)
		return nil, err
	}
	c.logger.Logf("Exec query: %s, args: %v", query, args)
	return r, nil
}

func (c *connWrapper) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	result, err := c.conn.(driver.ExecerContext).ExecContext(ctx, query, args)
	if err != nil {
		c.logger.Errorf("Failed to execute ExecContext: %v", err)
		return nil, err
	}
	c.logger.Logf("ExecContext with query: %s, args: %v", query, args)
	return result, nil
}

func (c *connWrapper) Query(query string, args []driver.Value) (driver.Rows, error) {
	rows, err := c.conn.(driver.Queryer).Query(query, args)
	if err != nil {
		c.logger.Errorf("Failed to execute query %s: %v", query, err)
		return nil, err
	}
	c.logger.Logf("Query query: %s, args: %v", query, args)
	return &rowsWrapper{rows: rows, logger: c.logger}, nil
}

func (c *connWrapper) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	rows, err := c.conn.(driver.QueryerContext).QueryContext(ctx, query, args)
	if err != nil {
		c.logger.Errorf("Failed to execute QueryContext: %v", err)
		return nil, err
	}
	c.logger.Logf("QueryContext with query: %s, args: %v", query, args)
	return &rowsWrapper{rows: rows, logger: c.logger}, nil
}

func (c *connWrapper) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	stmt, err := c.conn.(driver.ConnPrepareContext).PrepareContext(ctx, query)
	if err != nil {
		c.logger.Errorf("Failed to PrepareContext: %v", err)
		return nil, err
	}
	c.logger.Logf("Prepare context query: %s", query)
	return &stmtWrapper{stmt: stmt, logger: c.logger}, nil
}

func (c *connWrapper) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	tx, err := c.conn.(driver.ConnBeginTx).BeginTx(ctx, opts)
	if err != nil {
		c.logger.Errorf("Failed to begin transaction: %v", err)
		return nil, err
	}
	c.logger.Logf("Begin transaction with options: %+v", opts)
	return &txWrapper{tx: tx, logger: c.logger}, nil
}

func (c *connWrapper) ResetSession(ctx context.Context) error {
	err := c.conn.(driver.SessionResetter).ResetSession(ctx)
	if err != nil {
		c.logger.Errorf("Failed to reset session: %v", err)
		return err
	}
	c.logger.Logf("Successfully reset session")
	return nil
}

func (c *connWrapper) IsValid() bool {
	valid := c.conn.(driver.Validator).IsValid()
	c.logger.Logf("IsValid check: %v", valid)
	return valid
}

func (c *connWrapper) CheckNamedValue(value *driver.NamedValue) error {
	err := c.conn.(driver.NamedValueChecker).CheckNamedValue(value)
	if err != nil {
		c.logger.Errorf("Failed to CheckNamedValue statement: %v", err)
		return err
	}
	c.logger.Logf("CheckNamedValue statement with args: %v", value)
	return nil
}

func (c *connWrapper) Prepare(query string) (driver.Stmt, error) {
	stmt, err := c.conn.Prepare(query)
	if err != nil {
		c.logger.Errorf("Failed to Prepare query %s: %v", query, err)
		return nil, err
	}
	c.logger.Logf("Prepare query: %s", query)
	return &stmtWrapper{stmt: stmt, logger: c.logger}, nil
}

func (c *connWrapper) Close() error {
	err := c.conn.Close()
	if err != nil {
		c.logger.Errorf("Failed to Close connection: %v", err)
		return err
	}
	c.logger.Logf("Close connection")
	return nil
}

// Begin starts and returns a new transaction.
//
// Deprecated: Drivers should implement ConnBeginTx instead (or additionally).
func (c *connWrapper) Begin() (driver.Tx, error) {
	tx, err := c.conn.Begin()
	if err != nil {
		c.logger.Errorf("Failed to Begin transaction: %v", err)
		return nil, err
	}
	c.logger.Logf("Begin transaction")
	return &txWrapper{tx: tx, logger: c.logger}, nil
}

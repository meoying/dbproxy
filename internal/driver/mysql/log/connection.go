package log

import (
	"context"
	"database/sql/driver"
	"fmt"
)

type connWrapper struct {
	conn   driver.Conn
	dq     driver.QueryerContext
	logger Logger
}

func (c *connWrapper) Ping(ctx context.Context) error {
	// TODO implement me
	panic("implement me")
}

func (c *connWrapper) Exec(query string, args []driver.Value) (driver.Result, error) {
	// TODO implement me
	panic("implement me")
}

func (c *connWrapper) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	// TODO implement me
	panic("implement me")
}

func (c *connWrapper) Query(query string, args []driver.Value) (driver.Rows, error) {
	// TODO implement me
	panic("implement me")
}

func (c *connWrapper) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	// TODO implement me
	panic("implement me")
}

func (c *connWrapper) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	// TODO implement me
	panic("implement me")
}

func (c *connWrapper) ResetSession(ctx context.Context) error {
	// TODO implement me
	panic("implement me")
}

func (c *connWrapper) IsValid() bool {
	// TODO implement me
	panic("implement me")
}

func (c *connWrapper) CheckNamedValue(value *driver.NamedValue) error {
	// TODO implement me
	panic("implement me")
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

func (c *connWrapper) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	qc, ok := c.conn.(driver.QueryerContext)
	if !ok {
		c.logger.Errorf("The underlying connection does not implement QueryerContext")
		return nil, fmt.Errorf("%w", ErrNotImplementQueryerContext)
	}
	rows, err := qc.QueryContext(ctx, query, args)
	if err != nil {
		c.logger.Errorf("Failed to execute QueryContext: %v", err)
		return nil, err
	}
	c.logger.Logf("QueryContext with query: %s, args: %v", query, args)
	return &rowsWrapper{rows: rows, logger: c.logger}, nil
}

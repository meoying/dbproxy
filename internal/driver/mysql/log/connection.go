package log

import (
	"context"
	"database/sql/driver"
	"fmt"
)

type connWrapper struct {
	conn   driver.Conn
	logger Logger
}

func newConnWrapper(conn driver.Conn, logger Logger) (driver.Conn, error) {
	if _, ok := conn.(driver.QueryerContext); !ok {
		return nil, fmt.Errorf("%w", ErrNotImplementQueryerContext)
	}
	return &connWrapper{conn: conn, logger: logger}, nil
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
	rows, err := c.conn.(driver.QueryerContext).QueryContext(ctx, query, args)
	if err != nil {
		c.logger.Errorf("Failed to execute QueryContext: %v", err)
		return nil, err
	}
	c.logger.Logf("QueryContext with query: %s, args: %v", query, args)
	return &rowsWrapper{rows: rows, logger: c.logger}, nil
}

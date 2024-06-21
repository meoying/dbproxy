package log

import (
	"context"
	"database/sql/driver"
)

type connWrapper struct {
	conn   driver.Conn
	logger Logger
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

type connPrepareContextWrapper struct {
	driver.ConnPrepareContext
	logger Logger
}

func (c *connPrepareContextWrapper) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	stmt, err := c.ConnPrepareContext.PrepareContext(ctx, query)
	if err != nil {
		c.logger.Errorf("Failed to PrepareContext: %v", err)
		return nil, err
	}
	c.logger.Logf("Prepare context query: %s", query)
	return &stmtWrapper{stmt: stmt, logger: c.logger}, nil
}

type pingerWrapper struct {
	driver.Pinger
	logger Logger
}

func (p *pingerWrapper) Ping(ctx context.Context) error {
	err := p.Pinger.Ping(ctx)
	if err != nil {
		p.logger.Errorf("failed to ping: %v", err)
		return err
	}
	p.logger.Logf("successfully ping")
	return nil
}

type execContextWrapper struct {
	driver.ExecerContext
	logger Logger
}

func (e *execContextWrapper) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	result, err := e.ExecerContext.ExecContext(ctx, query, args)
	if err != nil {
		e.logger.Errorf("Failed to execute ExecContext: %v", err)
		return nil, err
	}
	e.logger.Logf("ExecContext with query: %s, args: %v", query, args)
	return result, nil
}

type queryerContextWrapper struct {
	driver.QueryerContext
	logger Logger
}

func (q *queryerContextWrapper) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	rows, err := q.QueryerContext.QueryContext(ctx, query, args)
	if err != nil {
		q.logger.Errorf("Failed to execute QueryContext: %v", err)
		return nil, err
	}
	q.logger.Logf("QueryContext with query: %s, args: %v", query, args)
	return &rowsWrapper{rows: rows, logger: q.logger}, nil
}

type connBeginTxWrapper struct {
	driver.ConnBeginTx
	logger Logger
}

func (c *connBeginTxWrapper) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	tx, err := c.ConnBeginTx.BeginTx(ctx, opts)
	if err != nil {
		c.logger.Errorf("Failed to begin transaction: %v", err)
		return nil, err
	}
	c.logger.Logf("Begin transaction with options: %+v", opts)
	return &txWrapper{tx: tx, logger: c.logger}, nil
}

type resetSessionWrapper struct {
	driver.SessionResetter
	logger Logger
}

func (r *resetSessionWrapper) ResetSession(ctx context.Context) error {
	err := r.SessionResetter.ResetSession(ctx)
	if err != nil {
		r.logger.Errorf("Failed to reset session: %v", err)
		return err
	}
	r.logger.Logf("Successfully reset session")
	return nil
}

type validatorWrapper struct {
	driver.Validator
	logger Logger
}

func (i *validatorWrapper) IsValid() bool {
	valid := i.Validator.IsValid()
	i.logger.Logf("IsValid check: %v", valid)
	return valid
}

type execerWrapper struct {
	driver.Execer
	logger Logger
}

func (e *execerWrapper) Exec(query string, args []driver.Value) (driver.Result, error) {
	r, err := e.Execer.Exec(query, args)
	if err != nil {
		e.logger.Errorf("Failed to execute query %s: %v", query, err)
		return nil, err
	}
	e.logger.Logf("Exec query: %s, args: %v", query, args)
	return r, nil
}

type queryerWrapper struct {
	driver.Queryer
	logger Logger
}

func (q *queryerWrapper) Query(query string, args []driver.Value) (driver.Rows, error) {
	rows, err := q.Queryer.Query(query, args)
	if err != nil {
		q.logger.Errorf("Failed to execute query %s: %v", query, err)
		return nil, err
	}
	q.logger.Logf("Query query: %s, args: %v", query, args)
	return &rowsWrapper{rows: rows, logger: q.logger}, nil
}

package log

import "database/sql/driver"

type txWrapper struct {
	tx     driver.Tx
	logger Logger
}

func (t *txWrapper) Commit() error {
	err := t.tx.Commit()
	if err != nil {
		t.logger.Error("failed to commit transaction", "error", err)
		return err
	}
	t.logger.Info("committed transaction")
	return nil
}

func (t *txWrapper) Rollback() error {
	err := t.tx.Rollback()
	if err != nil {
		t.logger.Error("failed to rollback transaction", "error", err)
		return err
	}
	t.logger.Info("rolled back transaction")
	return nil
}

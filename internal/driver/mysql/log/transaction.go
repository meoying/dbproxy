package log

import "database/sql/driver"

type txWrapper struct {
	tx     driver.Tx
	logger Logger
}

func (t *txWrapper) Commit() error {
	err := t.tx.Commit()
	if err != nil {
		t.logger.Errorf("Failed to Commit transaction: %v", err)
		return err
	}
	t.logger.Logf("Commit transaction")
	return nil
}

func (t *txWrapper) Rollback() error {
	err := t.tx.Rollback()
	if err != nil {
		t.logger.Errorf("Failed to Rollback transaction: %v", err)
		return err
	}
	t.logger.Logf("Rollback transaction")
	return nil
}

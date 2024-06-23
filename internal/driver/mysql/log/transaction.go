package log

import "database/sql/driver"

type txWrapper struct {
	tx     driver.Tx
	logger logger
}

func (t *txWrapper) Commit() error {
	err := t.tx.Commit()
	if err != nil {
		t.logger.Error("提交事务失败", "错误", err)
		return err
	}
	t.logger.Info("事务提交成功")
	return nil
}

func (t *txWrapper) Rollback() error {
	err := t.tx.Rollback()
	if err != nil {
		t.logger.Error("回滚事务失败", "错误", err)
		return err
	}
	t.logger.Info("事务回滚成功")
	return nil
}

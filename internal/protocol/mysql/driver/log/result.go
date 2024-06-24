package log

import (
	"database/sql/driver"

	driver2 "github.com/meoying/dbproxy/internal/protocol/mysql/driver"
)

var _ driver2.Result = &resultWrapper{}

type resultWrapper struct {
	result driver.Result
	logger logger
}

func (r *resultWrapper) LastInsertId() (int64, error) {
	id, err := r.result.LastInsertId()
	if err != nil {
		r.logger.Error("获取最后插入的ID失败", "错误", err)
		return 0, err
	}
	r.logger.Info("获取最后插入的ID成功", "ID", id)
	return id, nil
}

func (r *resultWrapper) RowsAffected() (int64, error) {
	rows, err := r.result.RowsAffected()
	if err != nil {
		r.logger.Error("获取受影响的行数失败", "错误", err)
		return 0, err
	}
	r.logger.Info("获取受影响的行数成功", "行数", rows)
	return rows, nil
}

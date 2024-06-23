package log

import (
	"database/sql/driver"

	"github.com/go-sql-driver/mysql"
)

type resultWrapper struct {
	result driver.Result
	logger logger
}

func (r *resultWrapper) AllRowsAffected() []int64 {
	affected := r.result.(mysql.Result).AllRowsAffected()
	r.logger.Info("获取所有受影响的行数", "行数", affected)
	return affected
}

func (r *resultWrapper) AllLastInsertIds() []int64 {
	ids := r.result.(mysql.Result).AllLastInsertIds()
	r.logger.Info("获取所有最后插入的ID", "IDs", ids)
	return ids
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

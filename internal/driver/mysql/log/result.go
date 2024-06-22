package log

import (
	"database/sql/driver"

	"github.com/go-sql-driver/mysql"
)

type resultWrapper struct {
	result driver.Result
	logger Logger
}

func (r *resultWrapper) AllRowsAffected() []int64 {
	affected := r.result.(mysql.Result).AllRowsAffected()
	r.logger.Info("all affected rows retrieved", "allRowsAffected", affected)
	return affected
}

func (r *resultWrapper) AllLastInsertIds() []int64 {
	ids := r.result.(mysql.Result).AllLastInsertIds()
	r.logger.Info("all last insert IDs retrieved", "ids", ids)
	return ids
}

func (r *resultWrapper) LastInsertId() (int64, error) {
	id, err := r.result.LastInsertId()
	if err != nil {
		r.logger.Error("failed to get last insert ID", "error", err)
		return 0, err
	}
	r.logger.Info("last insert ID retrieved", "id", id)
	return id, nil
}

func (r *resultWrapper) RowsAffected() (int64, error) {
	rows, err := r.result.RowsAffected()
	if err != nil {
		r.logger.Error("failed to get number of affected rows", "error", err)
		return 0, err
	}
	r.logger.Info("number of affected rows retrieved", "rowsAffected", rows)
	return rows, nil
}

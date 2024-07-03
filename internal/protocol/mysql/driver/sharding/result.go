package sharding

import (
	"database/sql"
)

type result struct {
	r sql.Result
}

func (r *result) LastInsertId() (int64, error) {
	return r.r.LastInsertId()
}

func (r *result) RowsAffected() (int64, error) {
	return r.r.RowsAffected()
}

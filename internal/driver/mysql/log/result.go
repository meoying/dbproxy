package log

import "database/sql/driver"

type resultWrapper struct {
	result driver.Result
	logger Logger
}

func (r *resultWrapper) AllRowsAffected() []int64 {
	// TODO implement me
	panic("implement me")
}

func (r *resultWrapper) AllLastInsertIds() []int64 {
	// TODO implement me
	panic("implement me")
}

func (r *resultWrapper) LastInsertId() (int64, error) {
	id, err := r.result.LastInsertId()
	if err != nil {
		r.logger.Errorf("Failed to get LastInsertId: %v", err)
		return 0, err
	}
	r.logger.Logf("LastInsertId: %d", id)
	return id, nil
}

func (r *resultWrapper) RowsAffected() (int64, error) {
	rows, err := r.result.RowsAffected()
	if err != nil {
		r.logger.Errorf("Failed to get RowsAffected: %v", err)
		return 0, err
	}
	r.logger.Logf("RowsAffected: %d", rows)
	return rows, nil
}

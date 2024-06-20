package log

import (
	"database/sql/driver"
	"reflect"
)

type rowsWrapper struct {
	rows   driver.Rows
	logger Logger
}

func (r *rowsWrapper) HasNextResultSet() bool {
	// TODO implement me
	panic("implement me")
}

func (r *rowsWrapper) NextResultSet() error {
	// TODO implement me
	panic("implement me")
}

func (r *rowsWrapper) ColumnTypeScanType(index int) reflect.Type {
	// TODO implement me
	panic("implement me")
}

func (r *rowsWrapper) ColumnTypeDatabaseTypeName(index int) string {
	// TODO implement me
	panic("implement me")
}

func (r *rowsWrapper) ColumnTypeNullable(index int) (nullable, ok bool) {
	// TODO implement me
	panic("implement me")
}

func (r *rowsWrapper) ColumnTypePrecisionScale(index int) (precision, scale int64, ok bool) {
	// TODO implement me
	panic("implement me")
}

func (r *rowsWrapper) Columns() []string {
	return r.rows.Columns()
}

func (r *rowsWrapper) Close() error {
	r.logger.Logf("Close rows")
	err := r.rows.Close()
	if err != nil {
		r.logger.Errorf("Failed to Close rows: %v", err)
		return err
	}
	return nil
}

func (r *rowsWrapper) Next(dest []driver.Value) error {
	err := r.rows.Next(dest)
	if err != nil {
		r.logger.Errorf("Failed to fetch next row: %v", err)
		return err
	}
	return nil
}

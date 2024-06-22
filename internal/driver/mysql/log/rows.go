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
	r.logger.Logf("HasNextResultSet\n")
	return r.rows.(driver.RowsNextResultSet).HasNextResultSet()
}

func (r *rowsWrapper) NextResultSet() error {
	err := r.rows.(driver.RowsNextResultSet).NextResultSet()
	if err != nil {
		r.logger.Errorf("Failed to fetch next result set: %v", err)
		return err
	}
	r.logger.Logf("Fetch next result set")
	return nil
}

func (r *rowsWrapper) ColumnTypeScanType(index int) reflect.Type {
	r.logger.Logf("ColumnTypeScanType: %d", index)
	return r.rows.(driver.RowsColumnTypeScanType).ColumnTypeScanType(index)
}

func (r *rowsWrapper) ColumnTypeDatabaseTypeName(index int) string {
	r.logger.Logf("ColumnTypeDatabaseTypeName: %d", index)
	return r.rows.(driver.RowsColumnTypeDatabaseTypeName).ColumnTypeDatabaseTypeName(index)
}

func (r *rowsWrapper) ColumnTypeNullable(index int) (nullable, ok bool) {
	r.logger.Logf("ColumnTypeNullable: %d", index)
	return r.rows.(driver.RowsColumnTypeNullable).ColumnTypeNullable(index)
}

func (r *rowsWrapper) ColumnTypePrecisionScale(index int) (precision, scale int64, ok bool) {
	r.logger.Logf("ColumnTypePrecisionScale: %d", index)
	return r.rows.(driver.RowsColumnTypePrecisionScale).ColumnTypePrecisionScale(index)
}

func (r *rowsWrapper) Columns() []string {
	cs := r.rows.Columns()
	r.logger.Logf("Columns: %v", cs)
	return cs
}

func (r *rowsWrapper) Close() error {
	err := r.rows.Close()
	if err != nil {
		r.logger.Errorf("Failed to Close rows: %v", err)
		return err
	}
	r.logger.Logf("Close rows")
	return nil
}

func (r *rowsWrapper) Next(dest []driver.Value) error {
	err := r.rows.Next(dest)
	if err != nil {
		r.logger.Errorf("Failed to fetch next row: %v", err)
		return err
	}
	r.logger.Logf("Fetch next row")
	return nil
}

package log

import (
	"database/sql/driver"
	"errors"
	"io"
	"reflect"
)

type rowsWrapper struct {
	rows   driver.Rows
	logger Logger
}

func (r *rowsWrapper) HasNextResultSet() bool {
	hasNext := r.rows.(driver.RowsNextResultSet).HasNextResultSet()
	r.logger.Info("checked for next result set", "hasNext", hasNext)
	return hasNext
}

func (r *rowsWrapper) NextResultSet() error {
	err := r.rows.(driver.RowsNextResultSet).NextResultSet()
	if err != nil {
		r.logger.Error("failed to fetch next result set", "error", err)
		return err
	}
	r.logger.Info("fetched next result set")
	return nil
}

func (r *rowsWrapper) ColumnTypeScanType(index int) reflect.Type {
	scanType := r.rows.(driver.RowsColumnTypeScanType).ColumnTypeScanType(index)
	r.logger.Info("retrieved column type scan type", "index", index, "scanType", scanType)
	return scanType
}

func (r *rowsWrapper) ColumnTypeDatabaseTypeName(index int) string {
	typeName := r.rows.(driver.RowsColumnTypeDatabaseTypeName).ColumnTypeDatabaseTypeName(index)
	r.logger.Info("retrieved column database type name", "index", index, "typeName", typeName)
	return typeName
}

func (r *rowsWrapper) ColumnTypeNullable(index int) (nullable, ok bool) {
	nullable, ok = r.rows.(driver.RowsColumnTypeNullable).ColumnTypeNullable(index)
	r.logger.Info("checked column nullability", "index", index, "nullable", nullable, "ok", ok)
	return nullable, ok
}

func (r *rowsWrapper) ColumnTypePrecisionScale(index int) (precision, scale int64, ok bool) {
	precision, scale, ok = r.rows.(driver.RowsColumnTypePrecisionScale).ColumnTypePrecisionScale(index)
	r.logger.Info("retrieved column precision and scale", "index", index, "precision", precision, "scale", scale, "ok", ok)
	return precision, scale, ok
}

func (r *rowsWrapper) Columns() []string {
	cs := r.rows.Columns()
	r.logger.Info("retrieved column names", "columns", cs)
	return cs
}

func (r *rowsWrapper) Close() error {
	err := r.rows.Close()
	if err != nil {
		r.logger.Error("failed to close rows", "error", err)
		return err
	}
	r.logger.Info("rows closed successfully")
	return nil
}

func (r *rowsWrapper) Next(dest []driver.Value) error {
	err := r.rows.Next(dest)
	if err != nil {
		if !errors.Is(err, io.EOF) {
			r.logger.Error("failed to fetch next row", "error", err)
		}
		return err
	}
	r.logger.Info("fetched next row")
	return nil
}

package log

import (
	"database/sql/driver"
	"reflect"
)

type rowsWrapper struct {
	rows   driver.Rows
	logger Logger
}

func newRowsWrapper(rows driver.Rows, logger Logger) *rowsWrapper {
	return &rowsWrapper{rows: rows, logger: logger}
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

type rowsColumnTypePrecisionScaleWrapper struct {
	*rowsWrapper
	driver.RowsColumnTypePrecisionScale
}

func newRowsColumnTypePrecisionScaleWrapper(r driver.RowsColumnTypePrecisionScale, logger Logger) *rowsColumnTypePrecisionScaleWrapper {
	return &rowsColumnTypePrecisionScaleWrapper{rowsWrapper: newRowsWrapper(r, logger), RowsColumnTypePrecisionScale: r}
}

func (r *rowsColumnTypePrecisionScaleWrapper) ColumnTypePrecisionScale(index int) (precision, scale int64, ok bool) {
	r.logger.Logf("ColumnTypePrecisionScale: %d", index)
	return r.RowsColumnTypePrecisionScale.ColumnTypePrecisionScale(index)
}

type rowsColumnTypeNullableWrapper struct {
	*rowsWrapper
	driver.RowsColumnTypeNullable
}

func newRowsColumnTypeNullableWrapper(r driver.RowsColumnTypeNullable, logger Logger) *rowsColumnTypeNullableWrapper {
	return &rowsColumnTypeNullableWrapper{rowsWrapper: newRowsWrapper(r, logger), RowsColumnTypeNullable: r}
}

func (r *rowsColumnTypeNullableWrapper) ColumnTypeNullable(index int) (nullable, ok bool) {
	r.logger.Logf("ColumnTypeNullable: %d", index)
	return r.RowsColumnTypeNullable.ColumnTypeNullable(index)
}

type rowsNextResultSetWrapper struct {
	*rowsWrapper
	driver.RowsNextResultSet
}

func newRowsNextResultSetWrapper(r driver.RowsNextResultSet, logger Logger) *rowsNextResultSetWrapper {
	return &rowsNextResultSetWrapper{rowsWrapper: newRowsWrapper(r, logger), RowsNextResultSet: r}
}

func (r *rowsNextResultSetWrapper) HasNextResultSet() bool {
	r.logger.Logf("HasNextResultSet\n")
	return r.RowsNextResultSet.HasNextResultSet()
}

func (r *rowsNextResultSetWrapper) NextResultSet() error {
	err := r.RowsNextResultSet.NextResultSet()
	if err != nil {
		r.logger.Errorf("Failed to fetch next result set: %v", err)
		return err
	}
	r.logger.Logf("Fetch next result set")
	return nil
}

type rowsColumnTypeScanTypeWrapper struct {
	*rowsWrapper
	driver.RowsColumnTypeScanType
}

func newRowsColumnTypeScanTypeWrapper(r driver.RowsColumnTypeScanType, logger Logger) *rowsColumnTypeScanTypeWrapper {
	return &rowsColumnTypeScanTypeWrapper{rowsWrapper: newRowsWrapper(r, logger), RowsColumnTypeScanType: r}
}

func (r *rowsColumnTypeScanTypeWrapper) ColumnTypeScanType(index int) reflect.Type {
	r.logger.Logf("ColumnTypeScanType: %d", index)
	return r.RowsColumnTypeScanType.ColumnTypeScanType(index)
}

type rowsColumnTypeDatabaseTypeNameWrapper struct {
	*rowsWrapper
	driver.RowsColumnTypeDatabaseTypeName
}

func newRowsColumnTypeDatabaseTypeNameWrapper(r driver.RowsColumnTypeDatabaseTypeName, logger Logger) *rowsColumnTypeDatabaseTypeNameWrapper {
	return &rowsColumnTypeDatabaseTypeNameWrapper{rowsWrapper: newRowsWrapper(r, logger), RowsColumnTypeDatabaseTypeName: r}
}

func (r *rowsColumnTypeDatabaseTypeNameWrapper) ColumnTypeDatabaseTypeName(index int) string {
	r.logger.Logf("ColumnTypeDatabaseTypeName: %d", index)
	return r.RowsColumnTypeDatabaseTypeName.ColumnTypeDatabaseTypeName(index)
}

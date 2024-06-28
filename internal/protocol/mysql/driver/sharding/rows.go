package sharding

import (
	"database/sql/driver"
	"fmt"
	"io"
	"log"
	"reflect"

	"github.com/ecodeclub/ekit/slice"
	"github.com/ecodeclub/ekit/sqlx"
)

type rows struct {
	sqlxRows sqlx.Rows
}

func (r *rows) Columns() []string {
	cols, _ := r.sqlxRows.Columns()
	return cols
}

func (r *rows) Close() error {
	return r.sqlxRows.Close()
}

func (r *rows) Next(dest []driver.Value) error {
	if !r.sqlxRows.Next() {
		return fmt.Errorf("%w", io.EOF)
	}
	values := slice.Map(dest, func(idx int, src driver.Value) any {
		return &src
	})
	if err := r.sqlxRows.Scan(values...); err != nil {
		log.Printf("err = %#v", err)
		return err
	}
	for i, v := range values {
		dest[i] = *v.(*driver.Value)
	}
	return nil
}

func (r *rows) ColumnTypeScanType(index int) reflect.Type {
	// TODO implement me
	panic("implement me")
}

func (r *rows) ColumnTypeDatabaseTypeName(index int) string {
	// TODO implement me
	panic("implement me")
}

func (r *rows) ColumnTypeNullable(index int) (nullable, ok bool) {
	// TODO implement me
	panic("implement me")
}

func (r *rows) ColumnTypePrecisionScale(index int) (precision, scale int64, ok bool) {
	// TODO implement me
	panic("implement me")
}

func (r *rows) HasNextResultSet() bool {
	return r.sqlxRows.NextResultSet()
}

func (r *rows) NextResultSet() error {
	if !r.HasNextResultSet() {
		return fmt.Errorf("%w", io.EOF)
	}
	return nil
}

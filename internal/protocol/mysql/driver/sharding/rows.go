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

// 不支持nextResSet
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
	panic("暂不支持,有需要可以提issue")
}

func (r *rows) ColumnTypeDatabaseTypeName(index int) string {
	panic("暂不支持,有需要可以提issue")
}

func (r *rows) ColumnTypeNullable(index int) (nullable, ok bool) {
	panic("暂不支持,有需要可以提issue")
}

func (r *rows) ColumnTypePrecisionScale(index int) (precision, scale int64, ok bool) {
	panic("暂不支持,有需要可以提issue")
}

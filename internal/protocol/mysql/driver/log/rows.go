package log

import (
	"database/sql/driver"
	"errors"
	"io"
	"reflect"

	driver2 "github.com/meoying/dbproxy/internal/protocol/mysql/driver"
)

var _ driver2.Rows = &rowsWrapper{}

type rowsWrapper struct {
	rows   driver.Rows
	logger logger
}

func (r *rowsWrapper) HasNextResultSet() bool {
	hasNext := r.rows.(driver.RowsNextResultSet).HasNextResultSet()
	r.logger.Info("检查是否有下一个结果集", "有", hasNext)
	return hasNext
}

func (r *rowsWrapper) NextResultSet() error {
	err := r.rows.(driver.RowsNextResultSet).NextResultSet()
	if err != nil {
		r.logger.Error("获取下一个结果集失败", "错误", err)
		return err
	}
	r.logger.Info("获取下一个结果集成功")
	return nil
}

func (r *rowsWrapper) ColumnTypeScanType(index int) reflect.Type {
	scanType := r.rows.(driver.RowsColumnTypeScanType).ColumnTypeScanType(index)
	r.logger.Info("获取列扫描类型", "索引", index, "扫描类型", scanType)
	return scanType
}

func (r *rowsWrapper) ColumnTypeDatabaseTypeName(index int) string {
	typeName := r.rows.(driver.RowsColumnTypeDatabaseTypeName).ColumnTypeDatabaseTypeName(index)
	r.logger.Info("获取列数据库类型名称", "索引", index, "类型名称", typeName)
	return typeName
}

func (r *rowsWrapper) ColumnTypeNullable(index int) (nullable, ok bool) {
	nullable, ok = r.rows.(driver.RowsColumnTypeNullable).ColumnTypeNullable(index)
	r.logger.Info("检查列是否可为空", "索引", index, "可为空", nullable, "ok", ok)
	return nullable, ok
}

func (r *rowsWrapper) ColumnTypePrecisionScale(index int) (precision, scale int64, ok bool) {
	precision, scale, ok = r.rows.(driver.RowsColumnTypePrecisionScale).ColumnTypePrecisionScale(index)
	r.logger.Info("获取列精度和刻度", "索引", index, "精度", precision, "刻度", scale, "ok", ok)
	return precision, scale, ok
}

func (r *rowsWrapper) Columns() []string {
	cs := r.rows.Columns()
	r.logger.Info("获取列名", "列名", cs)
	return cs
}

func (r *rowsWrapper) Close() error {
	err := r.rows.Close()
	if err != nil {
		r.logger.Error("关闭rows失败", "错误", err)
		return err
	}
	r.logger.Info("关闭rows成功")
	return nil
}

func (r *rowsWrapper) Next(dest []driver.Value) error {
	err := r.rows.Next(dest)
	if err != nil {
		if !errors.Is(err, io.EOF) {
			r.logger.Error("获取下一行失败", "错误", err)
		}
		return err
	}
	r.logger.Info("获取下一行成功")
	return nil
}

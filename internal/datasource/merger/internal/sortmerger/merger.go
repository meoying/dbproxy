// Copyright 2021 ecodeclub
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package sortmerger

import (
	"container/heap"
	"context"
	"database/sql"
	"fmt"
	"reflect"
	"sync"

	"github.com/meoying/dbproxy/internal/datasource/merger"
	heap2 "github.com/meoying/dbproxy/internal/datasource/merger/internal/sortmerger/heap"
	"github.com/meoying/dbproxy/internal/datasource/rows"

	"go.uber.org/multierr"

	"github.com/meoying/dbproxy/internal/datasource/merger/internal/errs"
)

// Merger  如果有GroupBy子句，会导致排序是给每个分组排的，那么该实现无法运作正常
type Merger struct {
	sortColumns merger.SortColumns
	cols        []string
	preScanAll  bool
}

// NewMerger 根据preScanAll及排序列的列信息来创建一个排序Merger
// 其中preScanAll为true 表示需要预先扫描出结果集中的所有数据到内存才能得到正确结果,为false每次只需要扫描一行即可得到正确结果
func NewMerger(preScanAll bool, sortCols ...merger.ColumnInfo) (*Merger, error) {
	scs, err := merger.NewSortColumns(sortCols...)
	if err != nil {
		return nil, err
	}
	return &Merger{
		preScanAll:  preScanAll,
		sortColumns: scs,
	}, nil
}

func (m *Merger) Merge(ctx context.Context, results []rows.Rows) (rows.Rows, error) {
	// 检测results是否符合条件
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	if len(results) == 0 {
		return nil, errs.ErrMergerEmptyRows
	}
	for i := 0; i < len(results); i++ {
		if err := m.checkColumns(results[i]); err != nil {
			return nil, err
		}
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
	}
	return m.initRows(results)
}

func (m *Merger) initRows(results []rows.Rows) (*Rows, error) {
	rs := &Rows{
		rowsList:     results,
		sortColumns:  m.sortColumns,
		mu:           &sync.RWMutex{},
		columns:      m.cols,
		isPreScanAll: m.preScanAll,
	}
	rs.hp = heap2.NewHeap(make([]*heap2.Node, 0, len(rs.rowsList)), rs.sortColumns)
	var err error
	// 下方preScanAll会把rowsList中所有数据扫描到内存然后关闭其中所有rows.Rows,所以要提前缓存住列类型信息
	columnTypes, err := rs.rowsList[0].ColumnTypes()
	if err != nil {
		return nil, err
	}
	rs.columnTypes = columnTypes
	for i := 0; i < len(rs.rowsList); i++ {
		if m.preScanAll {
			err = rs.preScanAll(rs.rowsList[i], i)
		} else {
			err = rs.preScanOne(rs.rowsList[i], i)
		}
		if err != nil {
			_ = rs.Close()
			return nil, err
		}
	}
	return rs, nil
}

func (m *Merger) checkColumns(rows rows.Rows) error {
	if rows == nil {
		return errs.ErrMergerRowsIsNull
	}
	cols, err := rows.Columns()
	if err != nil {
		return err
	}
	colMap := make(map[string]struct{}, len(cols))
	if len(m.cols) == 0 {
		m.cols = cols
	}
	if len(m.cols) != len(cols) {
		return errs.ErrMergerRowsDiff
	}
	for idx, colName := range cols {
		if m.cols[idx] != colName {
			return errs.ErrMergerRowsDiff
		}
		colMap[colName] = struct{}{}
	}

	for _, sortColumn := range m.sortColumns.Cols() {
		_, ok := colMap[sortColumn.SelectName()]
		if !ok {
			return errs.NewInvalidSortColumn(sortColumn.SelectName())
		}
	}
	return nil
}

func newNode(row rows.Rows, sortCols merger.SortColumns, index int) (*heap2.Node, error) {
	colsInfo, err := row.ColumnTypes()
	if err != nil {
		return nil, err
	}
	columns := make([]any, 0, len(colsInfo))
	sortColumns := make([]any, sortCols.Len())
	for _, colInfo := range colsInfo {
		colName := colInfo.Name()
		colType := colInfo.ScanType()
		for colType.Kind() == reflect.Ptr {
			colType = colType.Elem()
		}
		column := reflect.New(colType).Interface()
		if sortCols.Has(colName) {
			sortIndex := sortCols.Find(colName)
			sortColumns[sortIndex] = column
		}
		columns = append(columns, column)
	}
	err = row.Scan(columns...)
	if err != nil {
		return nil, err
	}
	for i := 0; i < len(sortColumns); i++ {
		sortColumns[i] = reflect.ValueOf(sortColumns[i]).Elem().Interface()
	}
	for i := 0; i < len(columns); i++ {
		columns[i] = reflect.ValueOf(columns[i]).Elem().Interface()
	}
	return &heap2.Node{
		RowsListIndex:    index,
		SortColumnValues: sortColumns,
		ColumnValues:     columns,
	}, nil
}

type Rows struct {
	rowsList     []rows.Rows
	columnTypes  []*sql.ColumnType
	sortColumns  merger.SortColumns
	hp           *heap2.Heap
	cur          *heap2.Node
	mu           *sync.RWMutex
	lastErr      error
	closed       bool
	columns      []string
	isPreScanAll bool
}

func (r *Rows) ColumnTypes() ([]*sql.ColumnType, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil, fmt.Errorf("%w", errs.ErrMergerRowsClosed)
	}
	return r.columnTypes, nil
}

func (*Rows) NextResultSet() bool {
	return false
}

func (r *Rows) Next() bool {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		return false
	}
	if r.hp.Len() == 0 || r.lastErr != nil {
		r.mu.Unlock()
		_ = r.Close()
		return false
	}
	r.cur = heap.Pop(r.hp).(*heap2.Node)
	if !r.isPreScanAll {
		row := r.rowsList[r.cur.RowsListIndex]
		err := r.preScanOne(row, r.cur.RowsListIndex)
		if err != nil {
			r.lastErr = err
			r.mu.Unlock()
			_ = r.Close()
			return false
		}
	}

	r.mu.Unlock()
	return true
}

func (r *Rows) preScanAll(row rows.Rows, index int) error {
	for row.Next() {
		n, err := newNode(row, r.sortColumns, index)
		if err != nil {
			return err
		}
		heap.Push(r.hp, n)
	}
	return row.Err()
}

func (r *Rows) preScanOne(row rows.Rows, index int) error {
	if row.Next() {
		n, err := newNode(row, r.sortColumns, index)
		if err != nil {
			return err
		}
		heap.Push(r.hp, n)
	} else if row.Err() != nil {
		return row.Err()
	}
	return nil
}

func (r *Rows) Scan(dest ...any) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.lastErr != nil {
		return r.lastErr
	}
	if r.closed {
		return errs.ErrMergerRowsClosed
	}
	if r.cur == nil {
		return errs.ErrMergerScanNotNext
	}
	var err error
	for i := 0; i < len(dest); i++ {
		err = rows.ConvertAssign(dest[i], r.cur.ColumnValues[i])
		if err != nil {
			return err
		}
	}
	return nil
}

func (r *Rows) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.closed = true
	errorList := make([]error, 0, len(r.rowsList))
	for i := 0; i < len(r.rowsList); i++ {
		row := r.rowsList[i]
		err := row.Close()
		if err != nil {
			errorList = append(errorList, err)
		}
	}
	return multierr.Combine(errorList...)
}

func (r *Rows) Err() error {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.lastErr
}

func (r *Rows) Columns() ([]string, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.closed {
		return nil, errs.ErrMergerRowsClosed
	}
	return r.columns, nil
}

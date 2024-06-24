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

package distinctmerger

import (
	"container/heap"
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"reflect"
	"sync"

	"github.com/ecodeclub/ekit/mapx"
	"github.com/ecodeclub/ekit/slice"
	"github.com/meoying/dbproxy/internal/merger"
	"github.com/meoying/dbproxy/internal/merger/internal/errs"
	heap2 "github.com/meoying/dbproxy/internal/merger/internal/sortmerger/heap"
	"github.com/meoying/dbproxy/internal/rows"
	"go.uber.org/multierr"
)

type treeMapKey struct {
	sortValues  []any
	values      []any
	sortColumns merger.SortColumns
}

func (k treeMapKey) isZeroValue() bool {
	return k.sortValues == nil && k.sortColumns.IsZeroValue()
}

func (k treeMapKey) compare(b treeMapKey) int {
	keyLen := len(k.sortValues)
	for i := 0; i < keyLen; i++ {
		var cmp func(any, any, merger.Order) int
		if _, ok := k.sortValues[i].(driver.Valuer); ok {
			cmp = merger.CompareNullable
		} else {
			kind := reflect.TypeOf(k.sortValues[i]).Kind()
			cmp = merger.CompareFuncMapping[kind]
		}
		res := cmp(k.sortValues[i], b.sortValues[i], k.sortColumns.Get(i).Order)
		if res != 0 {
			return res
		}
	}
	return 0
}

type Merger struct {
	sortColumns merger.SortColumns
	preScanAll  bool
	columnInfos []merger.ColumnInfo
	columnNames []string
}

func NewMerger(distinctCols []merger.ColumnInfo, sortColumns merger.SortColumns) (*Merger, error) {

	if len(distinctCols) == 0 {
		return nil, fmt.Errorf("%w", errs.ErrDistinctColsIsNull)
	}

	if sortColumns.IsZeroValue() {
		columns := slice.Map(distinctCols, func(idx int, src merger.ColumnInfo) merger.ColumnInfo {
			src.Order = merger.OrderASC
			return src
		})
		sortColumns, _ = merger.NewSortColumns(columns...)
		return &Merger{
			sortColumns: sortColumns,
			columnInfos: distinctCols,
			preScanAll:  true,
		}, nil
	}

	// 检查sortCols必须全在distinctCols
	var preScanAll bool
	distinctSet := make(map[string]struct{})
	for _, col := range distinctCols {
		name := col.SelectName()
		_, ok := distinctSet[name]
		if ok {
			return nil, fmt.Errorf("%w", errs.ErrDistinctColsRepeated)
		} else {
			distinctSet[name] = struct{}{}
		}
		// 补充缺少的排序列,最终达到排序列表与DISTINCT列表相同的效果
		if !sortColumns.Has(name) {
			preScanAll = true
			col.Order = merger.OrderASC
			sortColumns.Add(col)
		}
	}
	for i := 0; i < sortColumns.Len(); i++ {
		val := sortColumns.Get(i)
		if _, ok := distinctSet[val.SelectName()]; !ok {
			return nil, fmt.Errorf("%w", errs.ErrSortColListNotContainDistinctCol)
		}
	}
	return &Merger{
		sortColumns: sortColumns,
		columnInfos: distinctCols,
		preScanAll:  preScanAll,
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
		err := m.checkColumns(results[i])
		if err != nil {
			return nil, err
		}
	}
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	return m.initRows(results)
}

func (m *Merger) checkColumns(rows rows.Rows) error {
	if rows == nil {
		return errs.ErrMergerRowsIsNull
	}
	cols, err := rows.Columns()
	if err != nil {
		return err
	}
	// 判断数据库里的列只有去重列，且顺序要和定义的顺序一致
	if len(cols) != len(m.columnInfos) {
		return errs.ErrDistinctColsNotInCols
	}
	for _, distinctColumn := range m.columnInfos {
		if cols[distinctColumn.Index] != distinctColumn.SelectName() {
			return errs.ErrDistinctColsNotInCols
		}
	}
	m.columnNames = cols
	return err
}

func (m *Merger) initRows(results []rows.Rows) (*Rows, error) {
	r := &Rows{
		columnInfos: m.columnInfos,
		rowsList:    results,
		sortColumns: m.sortColumns,
		mu:          &sync.RWMutex{},
		columnNames: m.columnNames,
		preScanAll:  m.preScanAll,
	}
	r.hp = heap2.NewHeap(make([]*heap2.Node, 0, len(results)), m.sortColumns)

	t, err := mapx.NewTreeMap[treeMapKey, struct{}](func(src treeMapKey, dst treeMapKey) int {
		return src.compare(dst)
	})
	if err != nil {
		return nil, err
	}
	r.treeMap = t

	// 下方init会把rowsList中所有数据扫描到内存然后关闭其中所有rows.Rows,所以要提前缓存住列类型信息
	columnTypes, err := r.rowsList[0].ColumnTypes()
	if err != nil {
		return nil, err
	}
	r.columnTypes = columnTypes

	err = r.init()
	if err != nil {
		return nil, err
	}
	return r, nil
}

// 初始化堆和map，保证至少有一个排序列相同的所有数据全部拿出。第一个返回值表示results还有没有值
func (r *Rows) init() error {
	// 初始化将所有sql.Rows的第一个元素塞进heap中
	err := r.scanRowsIntoHeap()
	if err != nil {
		return err
	}
	// 如果四个results里面的元素均为空表示没有已经没有数据了
	_, err = r.deduplicate()
	return err
}

func (r *Rows) scanRowsIntoHeap() error {
	var err error
	for i := 0; i < len(r.rowsList); i++ {
		if r.preScanAll {
			err = r.preScanAllRows(i)
		} else {
			err = r.preScanOneRows(i)
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func (r *Rows) preScanAllRows(idx int) error {
	for r.rowsList[idx].Next() {
		n, err := r.newHeapNode(r.rowsList[idx], idx)
		if err != nil {
			return err
		}
		heap.Push(r.hp, n)
	}
	if r.rowsList[idx].Err() != nil {
		return r.rowsList[idx].Err()
	}
	return nil
}

func (r *Rows) preScanOneRows(idx int) error {
	if r.rowsList[idx].Next() {
		n, err := r.newHeapNode(r.rowsList[idx], idx)
		if err != nil {
			return err
		}
		heap.Push(r.hp, n)
	} else if r.rowsList[idx].Err() != nil {
		return r.rowsList[idx].Err()
	}
	return nil
}

func (r *Rows) newHeapNode(row rows.Rows, index int) (*heap2.Node, error) {
	colsInfo, err := row.ColumnTypes()
	if err != nil {
		return nil, err
	}
	columnValues := make([]any, 0, len(colsInfo))
	sortColumnValues := make([]any, r.sortColumns.Len())
	for _, colInfo := range colsInfo {
		colName := colInfo.Name()
		colType := colInfo.ScanType()
		for colType.Kind() == reflect.Ptr {
			colType = colType.Elem()
		}
		column := reflect.New(colType).Interface()
		if r.sortColumns.Has(colName) {
			sortIndex := r.sortColumns.Find(colName)
			sortColumnValues[sortIndex] = column
		}
		columnValues = append(columnValues, column)
	}
	err = row.Scan(columnValues...)
	if err != nil {
		return nil, err
	}
	for i := 0; i < len(sortColumnValues); i++ {
		v := reflect.ValueOf(sortColumnValues[i])
		if v.IsValid() && !v.IsZero() {
			sortColumnValues[i] = v.Elem().Interface()
		}
	}
	for i := 0; i < len(columnValues); i++ {
		columnValues[i] = reflect.ValueOf(columnValues[i]).Elem().Interface()
	}
	node := &heap2.Node{
		RowsListIndex:    index,
		SortColumnValues: sortColumnValues,
		ColumnValues:     columnValues,
	}
	return node, nil
}

func (r *Rows) deduplicate() (bool, error) {
	if r.preScanAll {
		return r.deduplicateAll()
	} else {
		return r.deduplicatePart()
	}
}

func (r *Rows) deduplicateAll() (bool, error) {
	for r.hp.Len() > 0 {
		node := heap.Pop(r.hp).(*heap2.Node)
		key := treeMapKey{
			sortValues:  node.SortColumnValues,
			values:      node.ColumnValues,
			sortColumns: r.sortColumns,
		}
		err := r.treeMap.Put(key, struct{}{})
		if err != nil {
			return false, err
		}
	}
	return true, nil
}

func (r *Rows) deduplicatePart() (bool, error) {
	var prevKey treeMapKey
	if r.hp.Len() == 0 {
		return false, nil
	}
	var indexes []int

	for {
		if r.hp.Len() == 0 {
			return r.preScanOneRowsByIndexes(indexes)
		}
		node := heap.Pop(r.hp).(*heap2.Node)
		if prevKey.isZeroValue() {
			prevKey = treeMapKey{
				sortValues:  node.SortColumnValues,
				values:      node.ColumnValues,
				sortColumns: r.sortColumns,
			}
		}

		// 相同元素进入treemap
		key := treeMapKey{
			sortValues:  node.SortColumnValues,
			values:      node.ColumnValues,
			sortColumns: r.sortColumns,
		}
		if key.compare(prevKey) == 0 {
			err := r.treeMap.Put(key, struct{}{})
			if err != nil {
				return false, err
			}
			// 将后续元素加入heap
			indexes = append(indexes, node.RowsListIndex)
		} else {
			// 如果排序列不相同将 拿出来的元素，重新塞进heap中
			heap.Push(r.hp, node)
			return r.preScanOneRowsByIndexes(indexes)
		}
	}
}

func (r *Rows) preScanOneRowsByIndexes(indexes []int) (bool, error) {
	// 从数据变动的rows.Rows中预扫描一行
	for _, index := range indexes {
		err := r.preScanOneRows(index)
		if err != nil {
			return false, err
		}
	}
	return false, nil
}

type Rows struct {
	columnInfos []merger.ColumnInfo
	rowsList    []rows.Rows
	columnTypes []*sql.ColumnType
	sortColumns merger.SortColumns
	hp          *heap2.Heap
	mu          *sync.RWMutex
	treeMap     *mapx.TreeMap[treeMapKey, struct{}]
	cur         []any
	closed      bool
	lastErr     error
	columnNames []string
	preScanAll  bool
}

func (r *Rows) NextResultSet() bool {
	return false
}

func (r *Rows) ColumnTypes() ([]*sql.ColumnType, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil, fmt.Errorf("%w", errs.ErrMergerRowsClosed)
	}
	return r.columnTypes, nil
}

func (r *Rows) Next() bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return false
	}
	if r.hp.Len() == 0 && len(r.treeMap.Keys()) == 0 || r.lastErr != nil {
		_ = r.close()
		return false
	}

	val := r.treeMap.Keys()[0]
	r.cur = val.values
	// 删除当前的数据行
	_, _ = r.treeMap.Delete(val)

	if len(r.treeMap.Keys()) == 0 {
		// 当一个排序列的数据取完就取下一个排序列的全部数据
		_, err := r.deduplicate()
		if err != nil {
			r.lastErr = err
			_ = r.close()
			return false
		}
	}

	return true
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
		err = rows.ConvertAssign(dest[i], r.cur[i])
		if err != nil {
			return err
		}
	}
	return nil
}

func (r *Rows) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.close()
}

func (r *Rows) close() error {
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

func (r *Rows) Columns() ([]string, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.closed {
		return nil, errs.ErrMergerRowsClosed
	}
	return r.columnNames, nil
}

func (r *Rows) Err() error {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.lastErr
}

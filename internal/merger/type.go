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

package merger

import (
	"context"
	"database/sql/driver"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/meoying/dbproxy/internal/merger/internal/errs"
	"github.com/meoying/dbproxy/internal/rows"
)

// Merger 将sql.Rows列表里的元素合并，返回一个类似sql.Rows的迭代器
// Merger sql.Rows列表中每个sql.Rows仅支持单个结果集且每个sql.Rows中列集必须完全相同。
type Merger interface {
	Merge(ctx context.Context, results []rows.Rows) (rows.Rows, error)
}

type Ordered interface {
	~int | ~int8 | ~int16 | ~int32 | ~int64 | ~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64 | ~float32 | ~float64 | ~string
}

type Order bool

const (
	// OrderASC 升序排序
	OrderASC Order = true
	// OrderDESC 降序排序
	OrderDESC Order = false
)

type ColumnInfo struct {
	Index         int
	Name          string
	AggregateFunc string
	Alias         string
	Order         Order
	Distinct      bool
}

func (c ColumnInfo) SelectName() string {
	if c.Alias != "" {
		return c.Alias
	}
	if c.AggregateFunc != "" {
		return fmt.Sprintf("%s(%s)", c.AggregateFunc, c.Name)
	}
	return c.Name
}

func (c ColumnInfo) Validate() bool {
	// ColumnInfo.Name中不能包含括号,也就是聚合函数, name = `id`, 而不是name = count(`id`)
	// 聚合函数需要写在aggregateFunc字段中
	return !strings.Contains(c.Name, "(")
}

// Compare 升序时， -1 表示 i < j, 1 表示i > j ,0 表示两者相同
// 降序时，-1 表示 i > j, 1 表示 i < j ,0 表示两者相同
func Compare[T Ordered](ii any, jj any, order Order) int {
	i, j := ii.(T), jj.(T)
	if i < j && order == OrderASC || i > j && order == OrderDESC {
		return -1
	} else if i > j && order == OrderASC || i < j && order == OrderDESC {
		return 1
	} else {
		return 0
	}
}

func CompareBool(ii, jj any, _ Order) int {
	i, j := ii.(bool), jj.(bool)
	if i == j {
		return 0
	}
	if i && !j {
		return 1
	}
	return -1
}

func CompareNullable(ii, jj any, order Order) int {
	i := ii.(driver.Valuer)
	j := jj.(driver.Valuer)
	iVal, _ := i.Value()
	jVal, _ := j.Value()
	// 如果i,j都为空返回0
	// 如果val返回为空永远是最小值
	if iVal == nil && jVal == nil {
		return 0
	} else if iVal == nil && order == OrderASC || jVal == nil && order == OrderDESC {
		return -1
	} else if iVal == nil && order == OrderDESC || jVal == nil && order == OrderASC {
		return 1
	}

	vi, ok := iVal.(time.Time)
	if ok {
		vj := jVal.(time.Time)
		return Compare[int64](vi.UnixMilli(), vj.UnixMilli(), order)
	}
	kind := reflect.TypeOf(iVal).Kind()
	return CompareFuncMapping[kind](iVal, jVal, order)
}

var CompareFuncMapping = map[reflect.Kind]func(any, any, Order) int{
	reflect.Int:     Compare[int],
	reflect.Int8:    Compare[int8],
	reflect.Int16:   Compare[int16],
	reflect.Int32:   Compare[int32],
	reflect.Int64:   Compare[int64],
	reflect.Uint8:   Compare[uint8],
	reflect.Uint16:  Compare[uint16],
	reflect.Uint32:  Compare[uint32],
	reflect.Uint64:  Compare[uint64],
	reflect.Float32: Compare[float32],
	reflect.Float64: Compare[float64],
	reflect.String:  Compare[string],
	reflect.Uint:    Compare[uint],
	reflect.Bool:    CompareBool,
}

type SortColumns struct {
	columns        []ColumnInfo
	selectName2Idx map[string]int
}

func NewSortColumns(sortCols ...ColumnInfo) (SortColumns, error) {
	if len(sortCols) == 0 {
		return SortColumns{}, errs.ErrEmptySortColumns
	}
	s := SortColumns{
		columns:        make([]ColumnInfo, 0, len(sortCols)),
		selectName2Idx: make(map[string]int, len(sortCols)),
	}
	// 这里索引表示的是排序列列表中的索引,而不是ColumnInfo中的Index(Index是SELECT列表中的顺序)
	for _, sortCol := range sortCols {
		name := sortCol.SelectName()
		if s.Has(name) {
			return SortColumns{}, errs.NewRepeatSortColumn(name)
		}
		s.Add(sortCol)
	}
	return s, nil
}

func (s *SortColumns) Has(name string) bool {
	_, ok := s.selectName2Idx[name]
	return ok
}

func (s *SortColumns) Add(column ColumnInfo) {
	name := column.SelectName()
	index := s.Len()
	if !s.Has(name) {
		s.columns = append(s.columns, column)
		s.selectName2Idx[name] = index
	}
}

func (s *SortColumns) Find(name string) int {
	return s.selectName2Idx[name]
}

func (s *SortColumns) Get(index int) ColumnInfo {
	return s.columns[index]
}

func (s *SortColumns) Len() int {
	return len(s.columns)
}

func (s *SortColumns) Cols() []ColumnInfo {
	return s.columns
}

func (s *SortColumns) IsZeroValue() bool {
	return s.columns == nil && s.selectName2Idx == nil
}

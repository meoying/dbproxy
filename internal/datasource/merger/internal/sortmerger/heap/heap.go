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

package heap

import (
	"container/heap"
	"database/sql/driver"
	"reflect"

	"github.com/meoying/dbproxy/internal/datasource/merger"
)

type Heap struct {
	nodes       []*Node
	sortColumns merger.SortColumns
}

func NewHeap(h []*Node, sortColumns merger.SortColumns) *Heap {
	hp := &Heap{nodes: h, sortColumns: sortColumns}
	heap.Init(hp)
	return hp
}

func (h *Heap) Len() int {
	return len(h.nodes)
}

func (h *Heap) Less(i, j int) bool {
	for k := 0; k < h.sortColumns.Len(); k++ {
		valueI := h.nodes[i].SortColumnValues[k]
		valueJ := h.nodes[j].SortColumnValues[k]
		_, ok := valueJ.(driver.Valuer)
		var cmp func(any, any, merger.Order) int
		if ok {
			cmp = merger.CompareNullable
		} else {
			kind := reflect.TypeOf(valueI).Kind()
			cmp = merger.CompareFuncMapping[kind]
		}
		res := cmp(valueI, valueJ, h.sortColumns.Get(k).Order)
		if res == 0 {
			continue
		}
		if res == -1 {
			return true
		}
		return false
	}
	return false
}

func (h *Heap) Swap(i, j int) {
	h.nodes[i], h.nodes[j] = h.nodes[j], h.nodes[i]
}

func (h *Heap) Push(x any) {
	h.nodes = append(h.nodes, x.(*Node))
}

func (h *Heap) Pop() any {
	v := h.nodes[len(h.nodes)-1]
	h.nodes = h.nodes[:len(h.nodes)-1]
	return v
}

type Node struct {
	RowsListIndex int
	// 用于排序列
	SortColumnValues []any
	// 完整的行中的所有列,不用于排序仅用于缓存行数据
	ColumnValues []any
}

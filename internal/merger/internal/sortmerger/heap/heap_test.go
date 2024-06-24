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
	"database/sql"
	"testing"
	"time"

	"github.com/meoying/dbproxy/internal/merger"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestNodes(sortColsList [][]any) []*Node {
	res := make([]*Node, 0, len(sortColsList))
	for _, sortCols := range sortColsList {
		n := &Node{
			SortColumnValues: sortCols,
		}
		res = append(res, n)
	}
	return res
}

func TestHeap(t *testing.T) {
	testcases := []struct {
		name      string
		nodes     func() []*Node
		wantNodes func() []*Node
		sortCols  func() merger.SortColumns
	}{
		{
			name: "单个列升序",
			nodes: func() []*Node {
				return newTestNodes([][]any{
					{2},
					{5},
					{6},
					{1},
					{0},
				})
			},
			wantNodes: func() []*Node {
				return newTestNodes([][]any{
					{0},
					{1},
					{2},
					{5},
					{6},
				})
			},
			sortCols: func() merger.SortColumns {
				sortCols, err := merger.NewSortColumns(merger.ColumnInfo{
					Name:  "id",
					Order: merger.OrderASC,
				})
				require.NoError(t, err)
				return sortCols
			},
		},
		{
			name: "单个列降序",
			nodes: func() []*Node {
				return newTestNodes([][]any{
					{2},
					{5},
					{6},
					{1},
					{0},
				})
			},
			wantNodes: func() []*Node {
				return newTestNodes([][]any{
					{6},
					{5},
					{2},
					{1},
					{0},
				})
			},
			sortCols: func() merger.SortColumns {
				sortCols, err := merger.NewSortColumns([]merger.ColumnInfo{{
					Name:  "id",
					Order: merger.OrderDESC,
				}}...)
				require.NoError(t, err)
				return sortCols
			},
		},
		{
			name: "三个列顺序：升序,降序,升序",
			nodes: func() []*Node {
				return newTestNodes([][]any{
					{2, "b", 1},
					{2, "a", 1},
					{2, "e", 2},
					{2, "e", 1},
					{2, "e", 3},
					{5, "b", 1},
					{5, "a", 1},
					{5, "e", 2},
					{5, "e", 1},
					{5, "e", 3},
					{1, "b", 1},
					{1, "a", 1},
					{1, "e", 2},
					{1, "e", 1},
					{1, "e", 3},
				})
			},
			wantNodes: func() []*Node {
				return newTestNodes([][]any{
					{1, "e", 1},
					{1, "e", 2},
					{1, "e", 3},
					{1, "b", 1},
					{1, "a", 1},
					{2, "e", 1},
					{2, "e", 2},
					{2, "e", 3},
					{2, "b", 1},
					{2, "a", 1},
					{5, "e", 1},
					{5, "e", 2},
					{5, "e", 3},
					{5, "b", 1},
					{5, "a", 1},
				})
			},
			sortCols: func() merger.SortColumns {
				sortCols, err := merger.NewSortColumns([]merger.ColumnInfo{{
					Name:  "id",
					Order: merger.OrderASC,
				}, {
					Name:  "name",
					Order: merger.OrderDESC,
				}, {
					Name:  "age",
					Order: merger.OrderASC,
				}}...)
				require.NoError(t, err)
				return sortCols
			},
		},
		{
			name: "三个列顺序：降序,升序,降序",
			nodes: func() []*Node {
				return newTestNodes([][]any{
					{2, "b", 1},
					{2, "a", 1},
					{2, "e", 2},
					{2, "e", 1},
					{2, "e", 3},
					{5, "b", 1},
					{5, "a", 1},
					{5, "e", 2},
					{5, "e", 1},
					{5, "e", 3},
					{1, "b", 1},
					{1, "a", 1},
					{1, "e", 2},
					{1, "e", 1},
					{1, "e", 3},
				})
			},
			wantNodes: func() []*Node {
				return newTestNodes([][]any{
					{5, "a", 1},
					{5, "b", 1},
					{5, "e", 3},
					{5, "e", 2},
					{5, "e", 1},
					{2, "a", 1},
					{2, "b", 1},
					{2, "e", 3},
					{2, "e", 2},
					{2, "e", 1},
					{1, "a", 1},
					{1, "b", 1},
					{1, "e", 3},
					{1, "e", 2},
					{1, "e", 1},
				})
			},
			sortCols: func() merger.SortColumns {
				sortCols, err := merger.NewSortColumns([]merger.ColumnInfo{{
					Name:  "id",
					Order: merger.OrderDESC,
				}, {
					Name:  "name",
					Order: merger.OrderASC,
				}, {
					Name:  "age",
					Order: merger.OrderDESC,
				}}...)
				require.NoError(t, err)
				return sortCols
			},
		},
		{
			name: "三个列的顺序: 升序,升序,降序",
			nodes: func() []*Node {
				return newTestNodes([][]any{
					{2, "b", 1},
					{2, "a", 1},
					{2, "e", 2},
					{2, "e", 1},
					{2, "e", 3},
					{5, "b", 1},
					{5, "a", 1},
					{5, "e", 2},
					{5, "e", 1},
					{5, "e", 3},
					{1, "b", 1},
					{1, "a", 1},
					{1, "e", 2},
					{1, "e", 1},
					{1, "e", 3},
				})
			},
			wantNodes: func() []*Node {
				return newTestNodes([][]any{
					{1, "a", 1},
					{1, "b", 1},
					{1, "e", 3},
					{1, "e", 2},
					{1, "e", 1},
					{2, "a", 1},
					{2, "b", 1},
					{2, "e", 3},
					{2, "e", 2},
					{2, "e", 1},
					{5, "a", 1},
					{5, "b", 1},
					{5, "e", 3},
					{5, "e", 2},
					{5, "e", 1},
				})
			},
			sortCols: func() merger.SortColumns {
				sortCols, err := merger.NewSortColumns([]merger.ColumnInfo{{
					Name:  "id",
					Order: merger.OrderASC,
				}, {
					Name:  "name",
					Order: merger.OrderASC,
				}, {
					Name:  "age",
					Order: merger.OrderDESC,
				}}...)
				require.NoError(t, err)
				return sortCols
			},
		},
		{
			name: "三个列的顺序: 降序,降序,升序",
			nodes: func() []*Node {
				return newTestNodes([][]any{
					{2, "b", 1},
					{2, "a", 1},
					{2, "e", 2},
					{2, "e", 1},
					{2, "e", 3},
					{5, "b", 1},
					{5, "a", 1},
					{5, "e", 2},
					{5, "e", 1},
					{5, "e", 3},
					{1, "b", 1},
					{1, "a", 1},
					{1, "e", 2},
					{1, "e", 1},
					{1, "e", 3},
				})
			},
			wantNodes: func() []*Node {
				return newTestNodes([][]any{
					{5, "e", 1},
					{5, "e", 2},
					{5, "e", 3},
					{5, "b", 1},
					{5, "a", 1},
					{2, "e", 1},
					{2, "e", 2},
					{2, "e", 3},
					{2, "b", 1},
					{2, "a", 1},
					{1, "e", 1},
					{1, "e", 2},
					{1, "e", 3},
					{1, "b", 1},
					{1, "a", 1},
				})
			},
			sortCols: func() merger.SortColumns {
				sortCols, err := merger.NewSortColumns([]merger.ColumnInfo{{
					Name:  "id",
					Order: merger.OrderDESC,
				}, {
					Name:  "name",
					Order: merger.OrderDESC,
				}, {
					Name:  "age",
					Order: merger.OrderASC,
				}}...)
				require.NoError(t, err)
				return sortCols
			},
		},
		{
			name: "三个列的顺序: 降序,降序,降序",
			nodes: func() []*Node {
				return newTestNodes([][]any{
					{2, "b", 1},
					{2, "a", 1},
					{2, "e", 2},
					{2, "e", 1},
					{2, "e", 3},
					{5, "b", 1},
					{5, "a", 1},
					{5, "e", 2},
					{5, "e", 1},
					{5, "e", 3},
					{1, "b", 1},
					{1, "a", 1},
					{1, "e", 2},
					{1, "e", 1},
					{1, "e", 3},
				})
			},
			wantNodes: func() []*Node {
				return newTestNodes([][]any{
					{5, "e", 3},
					{5, "e", 2},
					{5, "e", 1},
					{5, "b", 1},
					{5, "a", 1},
					{2, "e", 3},
					{2, "e", 2},
					{2, "e", 1},
					{2, "b", 1},
					{2, "a", 1},
					{1, "e", 3},
					{1, "e", 2},
					{1, "e", 1},
					{1, "b", 1},
					{1, "a", 1},
				})
			},
			sortCols: func() merger.SortColumns {
				sortCols, err := merger.NewSortColumns([]merger.ColumnInfo{{
					Name:  "id",
					Order: merger.OrderDESC,
				}, {
					Name:  "name",
					Order: merger.OrderDESC,
				}, {
					Name:  "age",
					Order: merger.OrderDESC,
				}}...)
				require.NoError(t, err)
				return sortCols
			},
		},
		{
			name: "三个列的顺序: 升序,升序,升序",
			nodes: func() []*Node {
				return newTestNodes([][]any{
					{2, "b", 1},
					{2, "a", 1},
					{2, "e", 2},
					{2, "e", 1},
					{2, "e", 3},
					{5, "b", 1},
					{5, "a", 1},
					{5, "e", 2},
					{5, "e", 1},
					{5, "e", 3},
					{1, "b", 1},
					{1, "a", 1},
					{1, "e", 2},
					{1, "e", 1},
					{1, "e", 3},
				})
			},
			wantNodes: func() []*Node {
				return newTestNodes([][]any{
					{1, "a", 1},
					{1, "b", 1},
					{1, "e", 1},
					{1, "e", 2},
					{1, "e", 3},
					{2, "a", 1},
					{2, "b", 1},
					{2, "e", 1},
					{2, "e", 2},
					{2, "e", 3},
					{5, "a", 1},
					{5, "b", 1},
					{5, "e", 1},
					{5, "e", 2},
					{5, "e", 3},
				})
			},
			sortCols: func() merger.SortColumns {
				sortCols, err := merger.NewSortColumns([]merger.ColumnInfo{{
					Name:  "id",
					Order: merger.OrderASC,
				}, {
					Name:  "name",
					Order: merger.OrderASC,
				}, {
					Name:  "age",
					Order: merger.OrderASC,
				}}...)
				require.NoError(t, err)
				return sortCols
			},
		},
	}
	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			h := NewHeap(tc.nodes(), tc.sortCols())
			res := make([]*Node, 0, h.Len())
			for h.Len() > 0 {
				res = append(res, heap.Pop(h).(*Node))
			}
			assert.Equal(t, tc.wantNodes(), res)
		})
	}

}

func TestHeap_Nullable(t *testing.T) {
	testcases := []struct {
		name      string
		nodes     func() []*Node
		wantNodes func() []*Node
		sortCols  func() merger.SortColumns
	}{
		{
			name: "sql.NullInt64 asc",
			nodes: func() []*Node {
				return newTestNodes([][]any{
					{sql.NullInt64{Int64: 5, Valid: true}},
					{sql.NullInt64{Int64: 1, Valid: true}},
					{sql.NullInt64{Int64: 3, Valid: true}},
					{sql.NullInt64{Int64: 2, Valid: true}},
					{sql.NullInt64{Int64: 10, Valid: false}},
				})
			},
			wantNodes: func() []*Node {
				return newTestNodes([][]any{
					{sql.NullInt64{Int64: 10, Valid: false}},
					{sql.NullInt64{Int64: 1, Valid: true}},
					{sql.NullInt64{Int64: 2, Valid: true}},
					{sql.NullInt64{Int64: 3, Valid: true}},
					{sql.NullInt64{Int64: 5, Valid: true}},
				})
			},
			sortCols: func() merger.SortColumns {
				sortCols, err := merger.NewSortColumns([]merger.ColumnInfo{{
					Name:  "id",
					Order: merger.OrderASC,
				}}...)
				require.NoError(t, err)
				return sortCols
			},
		},
		{
			name: "sql.NullInt64 desc",
			nodes: func() []*Node {
				return newTestNodes([][]any{
					{sql.NullInt64{Int64: 5, Valid: true}},
					{sql.NullInt64{Int64: 1, Valid: true}},
					{sql.NullInt64{Int64: 3, Valid: true}},
					{sql.NullInt64{Int64: 2, Valid: true}},
					{sql.NullInt64{Int64: 10, Valid: false}},
				})
			},
			wantNodes: func() []*Node {
				return newTestNodes([][]any{
					{sql.NullInt64{Int64: 5, Valid: true}},
					{sql.NullInt64{Int64: 3, Valid: true}},
					{sql.NullInt64{Int64: 2, Valid: true}},
					{sql.NullInt64{Int64: 1, Valid: true}},
					{sql.NullInt64{Int64: 10, Valid: false}},
				})
			},
			sortCols: func() merger.SortColumns {
				sortCols, err := merger.NewSortColumns([]merger.ColumnInfo{{
					Name:  "id",
					Order: merger.OrderDESC,
				}}...)
				require.NoError(t, err)
				return sortCols
			},
		},
		{
			name: "sql.NullString asc",
			nodes: func() []*Node {
				return newTestNodes([][]any{
					{sql.NullString{String: "ab", Valid: true}},
					{sql.NullString{String: "cd", Valid: true}},
					{sql.NullString{String: "bc", Valid: true}},
					{sql.NullString{String: "ba", Valid: true}},
					{sql.NullString{String: "z", Valid: false}},
				})
			},
			wantNodes: func() []*Node {
				return newTestNodes([][]any{
					{sql.NullString{String: "z", Valid: false}},
					{sql.NullString{String: "ab", Valid: true}},
					{sql.NullString{String: "ba", Valid: true}},
					{sql.NullString{String: "bc", Valid: true}},
					{sql.NullString{String: "cd", Valid: true}},
				})
			},
			sortCols: func() merger.SortColumns {
				sortCols, err := merger.NewSortColumns([]merger.ColumnInfo{{
					Name:  "name",
					Order: merger.OrderASC,
				}}...)
				require.NoError(t, err)
				return sortCols
			},
		},
		{
			name: "sql.NullString desc",
			nodes: func() []*Node {
				return newTestNodes([][]any{
					{sql.NullString{String: "ab", Valid: true}},
					{sql.NullString{String: "cd", Valid: true}},
					{sql.NullString{String: "bc", Valid: true}},
					{sql.NullString{String: "z", Valid: false}},
					{sql.NullString{String: "ba", Valid: true}},
				})
			},
			wantNodes: func() []*Node {
				return newTestNodes([][]any{
					{sql.NullString{String: "cd", Valid: true}},
					{sql.NullString{String: "bc", Valid: true}},
					{sql.NullString{String: "ba", Valid: true}},
					{sql.NullString{String: "ab", Valid: true}},
					{sql.NullString{String: "z", Valid: false}},
				})
			},
			sortCols: func() merger.SortColumns {
				sortCols, err := merger.NewSortColumns([]merger.ColumnInfo{{
					Name:  "name",
					Order: merger.OrderDESC,
				}}...)
				require.NoError(t, err)
				return sortCols
			},
		},
		{
			name: "sql.NullInt16 asc",
			nodes: func() []*Node {
				return newTestNodes([][]any{
					{sql.NullInt16{Int16: 5, Valid: true}},
					{sql.NullInt16{Int16: 1, Valid: true}},
					{sql.NullInt16{Int16: 3, Valid: true}},
					{sql.NullInt16{Int16: 2, Valid: true}},
					{sql.NullInt16{Int16: 10, Valid: false}},
				})
			},
			wantNodes: func() []*Node {
				return newTestNodes([][]any{
					{sql.NullInt16{Int16: 10, Valid: false}},
					{sql.NullInt16{Int16: 1, Valid: true}},
					{sql.NullInt16{Int16: 2, Valid: true}},
					{sql.NullInt16{Int16: 3, Valid: true}},
					{sql.NullInt16{Int16: 5, Valid: true}},
				})
			},
			sortCols: func() merger.SortColumns {
				sortCols, err := merger.NewSortColumns([]merger.ColumnInfo{{
					Name:  "id",
					Order: merger.OrderASC,
				}}...)
				require.NoError(t, err)
				return sortCols
			},
		},
		{
			name: "sql.NullInt16 desc",
			nodes: func() []*Node {
				return newTestNodes([][]any{
					{sql.NullInt16{Int16: 5, Valid: true}},
					{sql.NullInt16{Int16: 1, Valid: true}},
					{sql.NullInt16{Int16: 3, Valid: true}},
					{sql.NullInt16{Int16: 2, Valid: true}},
					{sql.NullInt16{Int16: 10, Valid: false}},
				})
			},
			wantNodes: func() []*Node {
				return newTestNodes([][]any{
					{sql.NullInt16{Int16: 5, Valid: true}},
					{sql.NullInt16{Int16: 3, Valid: true}},
					{sql.NullInt16{Int16: 2, Valid: true}},
					{sql.NullInt16{Int16: 1, Valid: true}},
					{sql.NullInt16{Int16: 10, Valid: false}},
				})
			},
			sortCols: func() merger.SortColumns {
				sortCols, err := merger.NewSortColumns([]merger.ColumnInfo{{
					Name:  "id",
					Order: merger.OrderDESC,
				}}...)
				require.NoError(t, err)
				return sortCols
			},
		},
		{
			name: "sql.NullInt32 asc",
			nodes: func() []*Node {
				return newTestNodes([][]any{
					{sql.NullInt32{Int32: 5, Valid: true}},
					{sql.NullInt32{Int32: 1, Valid: true}},
					{sql.NullInt32{Int32: 3, Valid: true}},
					{sql.NullInt32{Int32: 2, Valid: true}},
					{sql.NullInt32{Int32: 10, Valid: false}},
				})
			},
			wantNodes: func() []*Node {
				return newTestNodes([][]any{
					{sql.NullInt32{Int32: 10, Valid: false}},
					{sql.NullInt32{Int32: 1, Valid: true}},
					{sql.NullInt32{Int32: 2, Valid: true}},
					{sql.NullInt32{Int32: 3, Valid: true}},
					{sql.NullInt32{Int32: 5, Valid: true}},
				})
			},
			sortCols: func() merger.SortColumns {
				sortCols, err := merger.NewSortColumns([]merger.ColumnInfo{{
					Name:  "id",
					Order: merger.OrderASC,
				}}...)
				require.NoError(t, err)
				return sortCols
			},
		},
		{
			name: "sql.NullInt32 desc",
			nodes: func() []*Node {
				return newTestNodes([][]any{
					{sql.NullInt32{Int32: 5, Valid: true}},
					{sql.NullInt32{Int32: 1, Valid: true}},
					{sql.NullInt32{Int32: 3, Valid: true}},
					{sql.NullInt32{Int32: 2, Valid: true}},
					{sql.NullInt32{Int32: 10, Valid: false}},
				})
			},
			wantNodes: func() []*Node {
				return newTestNodes([][]any{
					{sql.NullInt32{Int32: 5, Valid: true}},
					{sql.NullInt32{Int32: 3, Valid: true}},
					{sql.NullInt32{Int32: 2, Valid: true}},
					{sql.NullInt32{Int32: 1, Valid: true}},
					{sql.NullInt32{Int32: 10, Valid: false}},
				})
			},
			sortCols: func() merger.SortColumns {
				sortCols, err := merger.NewSortColumns([]merger.ColumnInfo{{
					Name:  "id",
					Order: merger.OrderDESC,
				}}...)
				require.NoError(t, err)
				return sortCols
			},
		},
		{
			name: "sql.NullFloat64 asc",
			nodes: func() []*Node {
				return newTestNodes([][]any{
					{sql.NullFloat64{Float64: 5.0, Valid: true}},
					{sql.NullFloat64{Float64: 1.0, Valid: true}},
					{sql.NullFloat64{Float64: 3.0, Valid: true}},
					{sql.NullFloat64{Float64: 2.0, Valid: true}},
					{sql.NullFloat64{Float64: 10.0, Valid: false}},
				})
			},
			wantNodes: func() []*Node {
				return newTestNodes([][]any{
					{sql.NullFloat64{Float64: 10.0, Valid: false}},
					{sql.NullFloat64{Float64: 1.0, Valid: true}},
					{sql.NullFloat64{Float64: 2.0, Valid: true}},
					{sql.NullFloat64{Float64: 3.0, Valid: true}},
					{sql.NullFloat64{Float64: 5.0, Valid: true}},
				})
			},
			sortCols: func() merger.SortColumns {
				sortCols, err := merger.NewSortColumns([]merger.ColumnInfo{{
					Name:  "id",
					Order: merger.OrderASC,
				}}...)
				require.NoError(t, err)
				return sortCols
			},
		},
		{
			name: "sql.NullFloat64 desc",
			nodes: func() []*Node {
				return newTestNodes([][]any{
					{sql.NullFloat64{Float64: 5.0, Valid: true}},
					{sql.NullFloat64{Float64: 1.0, Valid: true}},
					{sql.NullFloat64{Float64: 3.0, Valid: true}},
					{sql.NullFloat64{Float64: 2.0, Valid: true}},
					{sql.NullFloat64{Float64: 10.0, Valid: false}},
				})
			},
			wantNodes: func() []*Node {
				return newTestNodes([][]any{
					{sql.NullFloat64{Float64: 5.0, Valid: true}},
					{sql.NullFloat64{Float64: 3.0, Valid: true}},
					{sql.NullFloat64{Float64: 2.0, Valid: true}},
					{sql.NullFloat64{Float64: 1.0, Valid: true}},
					{sql.NullFloat64{Float64: 10.0, Valid: false}},
				})
			},
			sortCols: func() merger.SortColumns {
				sortCols, err := merger.NewSortColumns([]merger.ColumnInfo{{
					Name:  "id",
					Order: merger.OrderDESC,
				}}...)
				require.NoError(t, err)
				return sortCols
			},
		},

		{
			name: "sql.NullTime asc",
			nodes: func() []*Node {
				return newTestNodes([][]any{
					{sql.NullTime{Time: func() time.Time {
						tm, err := time.ParseInLocation("2006-01-02 15:04:05", "2022-01-01 12:00:00", time.Local)
						require.NoError(t, err)
						return tm
					}(), Valid: true}},
					{sql.NullTime{Time: func() time.Time {
						tm, err := time.ParseInLocation("2006-01-02 15:04:05", "2022-01-02 12:00:00", time.Local)
						require.NoError(t, err)
						return tm
					}(), Valid: true}},
					{sql.NullTime{Time: func() time.Time {
						tm, err := time.ParseInLocation("2006-01-02 15:04:05", "2022-01-09 12:00:00", time.Local)
						require.NoError(t, err)
						return tm
					}(), Valid: true}},
					{sql.NullTime{Time: func() time.Time {
						tm, err := time.ParseInLocation("2006-01-02 15:04:05", "2022-01-01 11:00:00", time.Local)
						require.NoError(t, err)
						return tm
					}(), Valid: true}},
					{sql.NullTime{Time: func() time.Time {
						tm, err := time.ParseInLocation("2006-01-02 15:04:05", "2022-01-20 12:00:00", time.Local)
						require.NoError(t, err)
						return tm
					}(), Valid: false}},
				})
			},
			wantNodes: func() []*Node {
				return newTestNodes([][]any{
					{sql.NullTime{Time: func() time.Time {
						tm, err := time.ParseInLocation("2006-01-02 15:04:05", "2022-01-20 12:00:00", time.Local)
						require.NoError(t, err)
						return tm
					}(), Valid: false}},
					{sql.NullTime{Time: func() time.Time {
						tm, err := time.ParseInLocation("2006-01-02 15:04:05", "2022-01-01 11:00:00", time.Local)
						require.NoError(t, err)
						return tm
					}(), Valid: true}},
					{sql.NullTime{Time: func() time.Time {
						tm, err := time.ParseInLocation("2006-01-02 15:04:05", "2022-01-01 12:00:00", time.Local)
						require.NoError(t, err)
						return tm
					}(), Valid: true}},
					{sql.NullTime{Time: func() time.Time {
						tm, err := time.ParseInLocation("2006-01-02 15:04:05", "2022-01-02 12:00:00", time.Local)
						require.NoError(t, err)
						return tm
					}(), Valid: true}},
					{sql.NullTime{Time: func() time.Time {
						tm, err := time.ParseInLocation("2006-01-02 15:04:05", "2022-01-09 12:00:00", time.Local)
						require.NoError(t, err)
						return tm
					}(), Valid: true}},
				})
			},
			sortCols: func() merger.SortColumns {
				sortCols, err := merger.NewSortColumns([]merger.ColumnInfo{{
					Name:  "time",
					Order: merger.OrderASC,
				}}...)
				require.NoError(t, err)
				return sortCols
			},
		},
		{
			name: "sql.NullTime desc",
			nodes: func() []*Node {
				return newTestNodes([][]any{
					{sql.NullTime{Time: func() time.Time {
						tm, err := time.ParseInLocation("2006-01-02 15:04:05", "2022-01-01 12:00:00", time.Local)
						require.NoError(t, err)
						return tm
					}(), Valid: true}},
					{sql.NullTime{Time: func() time.Time {
						tm, err := time.ParseInLocation("2006-01-02 15:04:05", "2022-01-02 12:00:00", time.Local)
						require.NoError(t, err)
						return tm
					}(), Valid: true}},
					{sql.NullTime{Time: func() time.Time {
						tm, err := time.ParseInLocation("2006-01-02 15:04:05", "2022-01-09 12:00:00", time.Local)
						require.NoError(t, err)
						return tm
					}(), Valid: true}},
					{sql.NullTime{Time: func() time.Time {
						tm, err := time.ParseInLocation("2006-01-02 15:04:05", "2022-01-01 11:00:00", time.Local)
						require.NoError(t, err)
						return tm
					}(), Valid: true}},
					{sql.NullTime{Time: func() time.Time {
						tm, err := time.ParseInLocation("2006-01-02 15:04:05", "2022-01-20 12:00:00", time.Local)
						require.NoError(t, err)
						return tm
					}(), Valid: false}},
				})
			},
			wantNodes: func() []*Node {
				return newTestNodes([][]any{
					{sql.NullTime{Time: func() time.Time {
						tm, err := time.ParseInLocation("2006-01-02 15:04:05", "2022-01-09 12:00:00", time.Local)
						require.NoError(t, err)
						return tm
					}(), Valid: true}},
					{sql.NullTime{Time: func() time.Time {
						tm, err := time.ParseInLocation("2006-01-02 15:04:05", "2022-01-02 12:00:00", time.Local)
						require.NoError(t, err)
						return tm
					}(), Valid: true}},
					{sql.NullTime{Time: func() time.Time {
						tm, err := time.ParseInLocation("2006-01-02 15:04:05", "2022-01-01 12:00:00", time.Local)
						require.NoError(t, err)
						return tm
					}(), Valid: true}},
					{sql.NullTime{Time: func() time.Time {
						tm, err := time.ParseInLocation("2006-01-02 15:04:05", "2022-01-01 11:00:00", time.Local)
						require.NoError(t, err)
						return tm
					}(), Valid: true}},
					{sql.NullTime{Time: func() time.Time {
						tm, err := time.ParseInLocation("2006-01-02 15:04:05", "2022-01-20 12:00:00", time.Local)
						require.NoError(t, err)
						return tm
					}(), Valid: false}},
				})
			},
			sortCols: func() merger.SortColumns {
				sortCols, err := merger.NewSortColumns([]merger.ColumnInfo{{
					Name:  "time",
					Order: merger.OrderDESC,
				}}...)
				require.NoError(t, err)
				return sortCols
			},
		},
		{
			name: "sql.NullByte asc",
			nodes: func() []*Node {
				return newTestNodes([][]any{
					{sql.NullByte{Byte: 'a', Valid: true}},
					{sql.NullByte{Byte: 'c', Valid: true}},
					{sql.NullByte{Byte: 'b', Valid: true}},
					{sql.NullByte{Byte: 'k', Valid: true}},
					{sql.NullByte{Byte: 'z', Valid: false}},
				})
			},
			wantNodes: func() []*Node {
				return newTestNodes([][]any{
					{sql.NullByte{Byte: 'z', Valid: false}},
					{sql.NullByte{Byte: 'a', Valid: true}},
					{sql.NullByte{Byte: 'b', Valid: true}},
					{sql.NullByte{Byte: 'c', Valid: true}},
					{sql.NullByte{Byte: 'k', Valid: true}},
				})
			},
			sortCols: func() merger.SortColumns {
				sortCols, err := merger.NewSortColumns([]merger.ColumnInfo{{
					Name:  "byte",
					Order: merger.OrderASC,
				}}...)
				require.NoError(t, err)
				return sortCols
			},
		},
		{
			name: "sql.NullByte desc",
			nodes: func() []*Node {
				return newTestNodes([][]any{
					{sql.NullByte{Byte: 'a', Valid: true}},
					{sql.NullByte{Byte: 'c', Valid: true}},
					{sql.NullByte{Byte: 'b', Valid: true}},
					{sql.NullByte{Byte: 'k', Valid: true}},
					{sql.NullByte{Byte: 'z', Valid: false}},
				})
			},
			wantNodes: func() []*Node {
				return newTestNodes([][]any{
					{sql.NullByte{Byte: 'k', Valid: true}},
					{sql.NullByte{Byte: 'c', Valid: true}},
					{sql.NullByte{Byte: 'b', Valid: true}},
					{sql.NullByte{Byte: 'a', Valid: true}},
					{sql.NullByte{Byte: 'z', Valid: false}},
				})
			},
			sortCols: func() merger.SortColumns {
				sortCols, err := merger.NewSortColumns([]merger.ColumnInfo{{
					Name:  "byte",
					Order: merger.OrderDESC,
				}}...)
				require.NoError(t, err)
				return sortCols
			},
		},
	}
	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			h := NewHeap(tc.nodes(), tc.sortCols())
			res := make([]*Node, 0, h.Len())
			for h.Len() > 0 {
				res = append(res, heap.Pop(h).(*Node))
			}
			assert.Equal(t, tc.wantNodes(), res)
		})
	}
}

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
	"database/sql"
	"database/sql/driver"
	"reflect"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCompare(t *testing.T) {
	testcases := []struct {
		name    string
		values  []any
		order   Order
		wantVal int
		kind    reflect.Kind
	}{
		{
			name:    "int8 ASC 1,2",
			values:  []any{int8(1), int8(2)},
			order:   OrderASC,
			wantVal: -1,
			kind:    reflect.Int8,
		},
		{
			name:    "int8 DESC 1,2",
			values:  []any{int8(1), int8(2)},
			order:   OrderDESC,
			wantVal: 1,
			kind:    reflect.Int8,
		},
		{
			name:    "int8 ASC 2,1",
			values:  []any{int8(2), int8(1)},
			order:   OrderASC,
			wantVal: 1,
			kind:    reflect.Int8,
		},
		{
			name:    "int8 DESC 2,1",
			values:  []any{int8(2), int8(1)},
			order:   OrderDESC,
			wantVal: -1,
			kind:    reflect.Int8,
		},
		{
			name:    "int8 equal",
			values:  []any{int8(2), int8(2)},
			order:   OrderDESC,
			wantVal: 0,
			kind:    reflect.Int8,
		},
		{
			name:    "int16 ASC 1,2",
			values:  []any{int16(1), int16(2)},
			order:   OrderASC,
			wantVal: -1,
			kind:    reflect.Int16,
		},
		{
			name:    "int16 DESC 1,2",
			values:  []any{int16(1), int16(2)},
			order:   OrderDESC,
			wantVal: 1,
			kind:    reflect.Int16,
		},
		{
			name:    "int16 ASC 2,1",
			values:  []any{int16(2), int16(1)},
			order:   OrderASC,
			wantVal: 1,
			kind:    reflect.Int16,
		},
		{
			name:    "int16 DESC 2,1",
			values:  []any{int16(2), int16(1)},
			order:   OrderDESC,
			wantVal: -1,
			kind:    reflect.Int16,
		},
		{
			name:    "int16 equal",
			values:  []any{int16(2), int16(2)},
			order:   OrderDESC,
			wantVal: 0,
			kind:    reflect.Int16,
		},
		{
			name:    "int32 ASC 1,2",
			values:  []any{int32(1), int32(2)},
			order:   OrderASC,
			wantVal: -1,
			kind:    reflect.Int32,
		},
		{
			name:    "int32 DESC 1,2",
			values:  []any{int32(1), int32(2)},
			order:   OrderDESC,
			wantVal: 1,
			kind:    reflect.Int32,
		},
		{
			name:    "int32 ASC 2,1",
			values:  []any{int32(2), int32(1)},
			order:   OrderASC,
			wantVal: 1,
			kind:    reflect.Int32,
		},
		{
			name:    "int32 DESC 2,1",
			values:  []any{int32(2), int32(1)},
			order:   OrderDESC,
			wantVal: -1,
			kind:    reflect.Int32,
		},
		{
			name:    "int32 equal",
			values:  []any{int32(2), int32(2)},
			order:   OrderDESC,
			wantVal: 0,
			kind:    reflect.Int32,
		},
		{
			name:    "int64 ASC 1,2",
			values:  []any{int64(1), int64(02)},
			order:   OrderASC,
			wantVal: -1,
			kind:    reflect.Int64,
		},
		{
			name:    "int64 DESC 1,2",
			values:  []any{int64(1), int64(2)},
			order:   OrderDESC,
			wantVal: 1,
			kind:    reflect.Int64,
		},
		{
			name:    "int64 ASC 2,1",
			values:  []any{int64(2), int64(1)},
			order:   OrderASC,
			wantVal: 1,
			kind:    reflect.Int64,
		},
		{
			name:    "int64 DESC 2,1",
			values:  []any{int64(2), int64(1)},
			order:   OrderDESC,
			wantVal: -1,
			kind:    reflect.Int64,
		},
		{
			name:    "int64 equal",
			values:  []any{int64(2), int64(2)},
			order:   OrderDESC,
			wantVal: 0,
			kind:    reflect.Int64,
		},
		{
			name:    "uint8 ASC 1,2",
			values:  []any{uint8(1), uint8(2)},
			order:   OrderASC,
			wantVal: -1,
			kind:    reflect.Uint8,
		},
		{
			name:    "uint8 DESC 1,2",
			values:  []any{uint8(1), uint8(2)},
			order:   OrderDESC,
			wantVal: 1,
			kind:    reflect.Uint8,
		},
		{
			name:    "uint8 ASC 2,1",
			values:  []any{uint8(2), uint8(1)},
			order:   OrderASC,
			wantVal: 1,
			kind:    reflect.Uint8,
		},
		{
			name:    "uint8 DESC 2,1",
			values:  []any{uint8(2), uint8(1)},
			order:   OrderDESC,
			wantVal: -1,
			kind:    reflect.Uint8,
		},
		{
			name:    "uint8 equal",
			values:  []any{uint8(2), uint8(2)},
			order:   OrderDESC,
			wantVal: 0,
			kind:    reflect.Uint8,
		},

		{
			name:    "uint16 ASC 1,2",
			values:  []any{uint16(1), uint16(2)},
			order:   OrderASC,
			wantVal: -1,
			kind:    reflect.Uint16,
		},
		{
			name:    "uint16 DESC 1,2",
			values:  []any{uint16(1), uint16(2)},
			order:   OrderDESC,
			wantVal: 1,
			kind:    reflect.Uint16,
		},
		{
			name:    "uint16 ASC 2,1",
			values:  []any{uint16(2), uint16(1)},
			order:   OrderASC,
			wantVal: 1,
			kind:    reflect.Uint16,
		},
		{
			name:    "uint16 DESC 2,1",
			values:  []any{uint16(2), uint16(1)},
			order:   OrderDESC,
			wantVal: -1,
			kind:    reflect.Uint16,
		},
		{
			name:    "uint16 equal",
			values:  []any{uint16(2), uint16(2)},
			order:   OrderDESC,
			wantVal: 0,
			kind:    reflect.Uint16,
		},
		{
			name:    "uint32 ASC 1,2",
			values:  []any{uint32(1), uint32(2)},
			order:   OrderASC,
			wantVal: -1,
			kind:    reflect.Uint32,
		},
		{
			name:    "uint32 DESC 1,2",
			values:  []any{uint32(1), uint32(2)},
			order:   OrderDESC,
			wantVal: 1,
			kind:    reflect.Uint32,
		},
		{
			name:    "uint32 ASC 2,1",
			values:  []any{uint32(2), uint32(1)},
			order:   OrderASC,
			wantVal: 1,
			kind:    reflect.Uint32,
		},
		{
			name:    "uint32 DESC 2,1",
			values:  []any{uint32(2), uint32(1)},
			order:   OrderDESC,
			wantVal: -1,
			kind:    reflect.Uint32,
		},
		{
			name:    "uint32 equal",
			values:  []any{uint32(2), uint32(2)},
			order:   OrderDESC,
			wantVal: 0,
			kind:    reflect.Uint32,
		},
		{
			name:    "uint64 ASC 1,2",
			values:  []any{uint64(1), uint64(2)},
			order:   OrderASC,
			wantVal: -1,
			kind:    reflect.Uint64,
		},
		{
			name:    "uint64 DESC 1,2",
			values:  []any{uint64(1), uint64(2)},
			order:   OrderDESC,
			wantVal: 1,
			kind:    reflect.Uint64,
		},
		{
			name:    "uint64 ASC 2,1",
			values:  []any{uint64(2), uint64(1)},
			order:   OrderASC,
			wantVal: 1,
			kind:    reflect.Uint64,
		},
		{
			name:    "uint64 DESC 2,1",
			values:  []any{uint64(2), uint64(1)},
			order:   OrderDESC,
			wantVal: -1,
			kind:    reflect.Uint64,
		},
		{
			name:    "uint64 equal",
			values:  []any{uint64(2), uint64(2)},
			order:   OrderDESC,
			wantVal: 0,
			kind:    reflect.Uint64,
		},
		{
			name:    "float32 ASC 1,2",
			values:  []any{float32(1.1), float32(2.1)},
			order:   OrderASC,
			wantVal: -1,
			kind:    reflect.Float32,
		},
		{
			name:    "float32 DESC 1,2",
			values:  []any{float32(1.1), float32(2.1)},
			order:   OrderDESC,
			wantVal: 1,
			kind:    reflect.Float32,
		},
		{
			name:    "float32 ASC 2,1",
			values:  []any{float32(2), float32(1)},
			order:   OrderASC,
			wantVal: 1,
			kind:    reflect.Float32,
		},
		{
			name:    "float32 DESC 2,1",
			values:  []any{float32(2.1), float32(1.1)},
			order:   OrderDESC,
			wantVal: -1,
			kind:    reflect.Float32,
		},
		{
			name:    "float32 equal",
			values:  []any{float32(2.1), float32(2.1)},
			order:   OrderDESC,
			wantVal: 0,
			kind:    reflect.Float32,
		},
		{
			name:    "float64 ASC 1,2",
			values:  []any{1.1, 2.1},
			order:   OrderASC,
			wantVal: -1,
			kind:    reflect.Float64,
		},
		{
			name:    "float64 DESC 1,2",
			values:  []any{float64(1), float64(2)},
			order:   OrderDESC,
			wantVal: 1,
			kind:    reflect.Float64,
		},
		{
			name:    "float64 ASC 2,1",
			values:  []any{float64(2), float64(1)},
			order:   OrderASC,
			wantVal: 1,
			kind:    reflect.Float64,
		},
		{
			name:    "float64 DESC 2,1",
			values:  []any{2.1, 1.1},
			order:   OrderDESC,
			wantVal: -1,
			kind:    reflect.Float64,
		},
		{
			name:    "float64 equal",
			values:  []any{2.1, 2.1},
			order:   OrderDESC,
			wantVal: 0,
			kind:    reflect.Float64,
		},
		{
			name:    "string equal",
			values:  []any{"x", "x"},
			order:   OrderDESC,
			wantVal: 0,
			kind:    reflect.String,
		},
	}
	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			cmp, ok := CompareFuncMapping[tc.kind]
			require.True(t, ok)
			val := cmp(tc.values[0], tc.values[1], tc.order)
			assert.Equal(t, tc.wantVal, val)
		})
	}
}

func TestSortColumns(t *testing.T) {
	t.Run("零值", func(t *testing.T) {
		s := SortColumns{}
		require.True(t, s.IsZeroValue())
	})
}

func TestCompareNullable(t *testing.T) {
	tests := []struct {
		name     string
		i        driver.Valuer
		j        driver.Valuer
		order    Order
		expected int
	}{
		{
			name:     "IntASC",
			i:        sql.NullInt64{Int64: 5, Valid: true},
			j:        sql.NullInt64{Int64: 10, Valid: true},
			order:    OrderASC,
			expected: -1,
		},
		{
			name:     "IntDESC",
			i:        sql.NullInt64{Int64: 10, Valid: true},
			j:        sql.NullInt64{Int64: 5, Valid: true},
			order:    OrderDESC,
			expected: -1,
		},
		{
			name:     "FloatASC",
			i:        sql.NullFloat64{Float64: 5.5, Valid: true},
			j:        sql.NullFloat64{Float64: 5.5, Valid: true},
			order:    OrderASC,
			expected: 0,
		},
		{
			name:     "StringASC",
			i:        sql.NullString{String: "abc", Valid: true},
			j:        sql.NullString{String: "xyz", Valid: true},
			order:    OrderASC,
			expected: -1,
		},
		{
			name:     "StringDESC",
			i:        sql.NullString{String: "xyz", Valid: true},
			j:        sql.NullString{String: "abc", Valid: true},
			order:    OrderDESC,
			expected: -1,
		},
		{
			name:     "both nil",
			i:        sql.NullInt64{},
			j:        sql.NullInt64{},
			order:    OrderASC,
			expected: 0,
		},
		{
			name:     "i nil, j not nil, ASC",
			i:        sql.NullInt64{},
			j:        sql.NullInt64{Valid: true, Int64: 10},
			order:    OrderASC,
			expected: -1,
		},
		{
			name:     "i nil, j not nil, DESC",
			i:        sql.NullInt64{},
			j:        sql.NullInt64{Valid: true, Int64: 10},
			order:    OrderDESC,
			expected: 1,
		},
		{
			name:     "i not nil, j nil, ASC",
			i:        sql.NullInt64{Valid: true, Int64: 10},
			j:        sql.NullInt64{},
			order:    OrderASC,
			expected: 1,
		},
		{
			name:     "i not nil, j nil, DESC",
			i:        sql.NullInt64{Valid: true, Int64: 10},
			j:        sql.NullInt64{},
			order:    OrderDESC,
			expected: -1,
		},
		{
			name:     "i < j, ASC",
			i:        sql.NullInt64{Valid: true, Int64: 5},
			j:        sql.NullInt64{Valid: true, Int64: 10},
			order:    OrderASC,
			expected: -1,
		},
		{
			name:     "i < j, DESC",
			i:        sql.NullInt64{Valid: true, Int64: 5},
			j:        sql.NullInt64{Valid: true, Int64: 10},
			order:    OrderDESC,
			expected: 1,
		},
		{
			name:     "i > j, ASC",
			i:        sql.NullInt64{Valid: true, Int64: 15},
			j:        sql.NullInt64{Valid: true, Int64: 10},
			order:    OrderASC,
			expected: 1,
		},
		{
			name:     "i > j, DESC",
			i:        sql.NullInt64{Valid: true, Int64: 15},
			j:        sql.NullInt64{Valid: true, Int64: 10},
			order:    OrderDESC,
			expected: -1,
		},
		{
			name:     "i == j, ASC",
			i:        sql.NullInt64{Valid: true, Int64: 10},
			j:        sql.NullInt64{Valid: true, Int64: 10},
			order:    OrderASC,
			expected: 0,
		},
		{
			name:     "i == j, DESC",
			i:        sql.NullInt64{Valid: true, Int64: 10},
			j:        sql.NullInt64{Valid: true, Int64: 10},
			order:    OrderDESC,
			expected: 0,
		},
		{
			name:     "i time < j time, ASC",
			i:        sql.NullTime{Valid: true, Time: time.Now()},
			j:        sql.NullTime{Valid: true, Time: time.Now().Add(time.Hour)},
			order:    OrderASC,
			expected: -1,
		},
		{
			name:     "i time < j time, DESC",
			i:        sql.NullTime{Valid: true, Time: time.Now()},
			j:        sql.NullTime{Valid: true, Time: time.Now().Add(time.Hour)},
			order:    OrderDESC,
			expected: 1,
		},
		{
			name:     "i time > j time, ASC",
			i:        sql.NullTime{Valid: true, Time: time.Now().Add(time.Hour)},
			j:        sql.NullTime{Valid: true, Time: time.Now()},
			order:    OrderASC,
			expected: 1,
		},
		{
			name:     "i time > j time, DESC",
			i:        sql.NullTime{Valid: true, Time: time.Now().Add(time.Hour)},
			j:        sql.NullTime{Valid: true, Time: time.Now()},
			order:    OrderDESC,
			expected: -1,
		},
		{
			name:     "i time == j time, ASC",
			i:        sql.NullTime{Valid: true, Time: time.Now()},
			j:        sql.NullTime{Valid: true, Time: time.Now()},
			order:    OrderASC,
			expected: 0,
		},
		{
			name:     "i time == j time, DESC",
			i:        sql.NullTime{Valid: true, Time: time.Now()},
			j:        sql.NullTime{Valid: true, Time: time.Now()},
			order:    OrderDESC,
			expected: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := CompareNullable(tt.i, tt.j, tt.order)
			assert.Equal(t, tt.expected, result)
		})
	}
}

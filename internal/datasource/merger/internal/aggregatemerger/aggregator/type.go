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

package aggregator

import (
	"database/sql/driver"
	"reflect"

	"github.com/meoying/dbproxy/internal/datasource/merger"
)

type AggregateElement interface {
	~int | ~int8 | ~int16 | ~int32 | ~int64 | ~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64 | ~float32 | ~float64
}

type Aggregator interface {
	// Aggregate 将多个列聚合 cols表示sqlRows列表里的数据，聚合函数通过下标拿到需要的列
	Aggregate(cols [][]any) (any, error)
	// ColumnInfo 聚合列的信息
	ColumnInfo() merger.ColumnInfo
	// Name 聚合函数本身的名称, MIN/MAX/SUM/COUNT/AVG
	Name() string
}

// nullableAggregator 处理查询到的nullable类型的数据，第一个返回值为 非null的数据 如果是sql.nullfloat64{value: 1.1,valid: true},返回的就是1.1,第二个返回值为value的kind
func nullableAggregator(colsData [][]any, index int) ([][]any, reflect.Kind) {
	notNullCols := make([][]any, 0, len(colsData))
	var kind reflect.Kind
	for _, colData := range colsData {
		col := colData[index]
		if reflect.TypeOf(col).Kind() == reflect.Struct {
			maxVal, _ := col.(driver.Valuer).Value()
			if maxVal != nil {
				kind = reflect.TypeOf(maxVal).Kind()
				colData[index] = maxVal
				notNullCols = append(notNullCols, colData)
			}
		} else {
			kind = reflect.TypeOf(col).Kind()
			notNullCols = append(notNullCols, colData)
		}
	}
	return notNullCols, kind
}

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

package column

// DataColumn 直接传入数据，伪装成了一个 Column
// 非线程安全实现
type DataColumn struct {
	name     string
	dataType string
}

func (ci *DataColumn) Name() string {
	return ci.name
}

func (ci *DataColumn) DatabaseTypeName() string {
	return ci.dataType
}

func NewColumn(name string, dataType string) *DataColumn {
	return &DataColumn{
		name:     name,
		dataType: dataType,
	}
}

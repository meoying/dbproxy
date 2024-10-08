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

package query

import "fmt"

type Query struct {
	SQL        string
	Args       []any
	DB         string
	Table      string
	Datasource string
}

func (q Query) String() string {
	return fmt.Sprintf("SQL: %s\nArgs: %#v\n", q.SQL, q.Args)
}

type Feature int

const (
	AggregateFunc Feature = 1 << iota
	GroupBy
	Distinct
	OrderBy
	Limit
)

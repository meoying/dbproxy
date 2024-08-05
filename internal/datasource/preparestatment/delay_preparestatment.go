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

package preparestatment

import (
	"context"
	"sync"

	"github.com/meoying/dbproxy/internal/datasource"
	"github.com/meoying/dbproxy/internal/query"
)

type DelayStmt struct {
	stmts sync.Map
}

func (s *DelayStmt) Load(ctx context.Context, query datasource.Query) (datasource.Stmt, bool) {
	stmt, ok := s.stmts.Load(s.getKey(query))
	if !ok {
		return nil, ok
	}
	return stmt.(datasource.Stmt), ok
}

func (s *DelayStmt) Store(ctx context.Context, query datasource.Query, stmt datasource.Stmt) {
	s.stmts.Store(s.getKey(query), stmt)
}

func (s *DelayStmt) getKey(query query.Query) string {
	return query.Datasource + "." + query.DB + "." + query.Table
}

func NewDelayStmt() *DelayStmt {
	return &DelayStmt{
		stmts: sync.Map{},
	}
}

var Stmts = NewDelayStmt()

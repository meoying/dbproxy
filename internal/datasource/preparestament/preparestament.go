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

package preparestament

import (
	"context"
	"database/sql"
	"github.com/meoying/dbproxy/internal/datasource"
)

var _ datasource.Stmt = &Prep{}

type Prep struct {
	stmt *sql.Stmt
}

func (stmt *Prep) Query(ctx context.Context, query datasource.Query) (*sql.Rows, error) {
	return stmt.stmt.QueryContext(ctx, query.Args...)
}

func (stmt *Prep) Exec(ctx context.Context, query datasource.Query) (sql.Result, error) {
	return stmt.stmt.ExecContext(ctx, query.Args)
}

func (stmt *Prep) Close() error {
	return stmt.stmt.Close()
}

func NewPrep(stmt *sql.Stmt) *Prep {
	return &Prep{stmt: stmt}
}

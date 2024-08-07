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

package transaction

import (
	"context"
	"database/sql"

	"github.com/meoying/dbproxy/internal/datasource"
	"github.com/meoying/dbproxy/internal/datasource/internal/statement"
)

var _ datasource.Tx = &Tx{}

// Tx 直接就是数
type Tx struct {
	tx *sql.Tx
}

func NewTx(tx *sql.Tx) *Tx {
	return &Tx{tx: tx}
}

func (t *Tx) Query(ctx context.Context, query datasource.Query) (*sql.Rows, error) {
	return t.tx.QueryContext(ctx, query.SQL, query.Args...)
}

func (t *Tx) Exec(ctx context.Context, query datasource.Query) (sql.Result, error) {
	return t.tx.ExecContext(ctx, query.SQL, query.Args...)
}

func (t *Tx) Prepare(ctx context.Context, query datasource.Query) (datasource.Stmt, error) {
	stmt, err := t.tx.PrepareContext(ctx, query.SQL)
	return statement.NewPreparedStatement(stmt), err
}

func (t *Tx) Commit() error {
	return t.tx.Commit()
}

func (t *Tx) Rollback() error {
	return t.tx.Rollback()
}

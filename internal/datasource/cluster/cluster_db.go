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

package cluster

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/meoying/dbproxy/internal/datasource/transaction"

	"github.com/meoying/dbproxy/internal/datasource"
	"github.com/meoying/dbproxy/internal/datasource/internal/errs"
	"github.com/meoying/dbproxy/internal/datasource/masterslave"
	"go.uber.org/multierr"
)

var (
	_ datasource.TxBeginner   = &clusterDB{}
	_ datasource.DataSource   = &clusterDB{}
	_ datasource.Finder       = &clusterDB{}
	_ datasource.StmtPreparer = &clusterDB{}
)

// clusterDB 以 DB 名称作为索引目标数据库
type clusterDB struct {
	// DataSource  应实现为 *masterSlavesDB
	masterSlavesDBs map[string]*masterslave.MasterSlavesDB
}

func NewClusterDB(ms map[string]*masterslave.MasterSlavesDB) datasource.DataSource {
	return &clusterDB{masterSlavesDBs: ms}
}

func (c *clusterDB) Query(ctx context.Context, query datasource.Query) (*sql.Rows, error) {
	ms, err := c.getTgt(query)
	if err != nil {
		return nil, err
	}
	return ms.Query(ctx, query)
}

func (c *clusterDB) Exec(ctx context.Context, query datasource.Query) (sql.Result, error) {
	ms, err := c.getTgt(query)
	if err != nil {
		return nil, err
	}
	return ms.Exec(ctx, query)
}

func (c *clusterDB) Prepare(ctx context.Context, query datasource.Query) (datasource.Stmt, error) {
	ms, err := c.getTgt(query)
	if err != nil {
		return nil, err
	}
	return ms.Prepare(ctx, query)
}

func (c *clusterDB) Close() error {
	var err error
	for name, inst := range c.masterSlavesDBs {
		if er := inst.Close(); er != nil {
			err = multierr.Combine(
				err, fmt.Errorf("masterslave DB name [%s] error: %w", name, er))
		}
	}
	return err
}

func (c *clusterDB) FindTgt(_ context.Context, query datasource.Query) (datasource.DataSource, error) {
	db, err := c.getTgt(query)
	if err != nil {
		return nil, err
	}
	return db, nil
}

func (c *clusterDB) getTgt(query datasource.Query) (*masterslave.MasterSlavesDB, error) {
	db, ok := c.masterSlavesDBs[query.DB]
	if !ok {
		return nil, errs.NewErrNotFoundTargetDB(query.DB)
	}
	return db, nil
}

func (c *clusterDB) BeginTx(ctx context.Context, opts *sql.TxOptions) (datasource.Tx, error) {
	facade, err := transaction.NewTxFacade(ctx, c)
	if err != nil {
		return nil, err
	}

	return facade.BeginTx(ctx, opts)
}

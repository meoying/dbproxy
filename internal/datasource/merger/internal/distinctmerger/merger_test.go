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

package distinctmerger

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	_ "github.com/mattn/go-sqlite3"
	"github.com/meoying/dbproxy/internal/datasource/merger"
	"github.com/meoying/dbproxy/internal/datasource/merger/internal/errs"
	"github.com/meoying/dbproxy/internal/datasource/rows"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"go.uber.org/multierr"
)

var mockErr = errors.New("mock error")

func TestMerger_NewMerger(t *testing.T) {
	testcases := []struct {
		name         string
		sortColsFunc func(t *testing.T) merger.SortColumns
		distinctCols []merger.ColumnInfo
		wantErr      error
	}{
		{
			name: "应该返回merger_去重列不为空且不含重复列_排序列表与去重列相同",
			sortColsFunc: func(t *testing.T) merger.SortColumns {
				t.Helper()
				columns := []merger.ColumnInfo{
					{
						Index: 0,
						Name:  "column1",
						Order: merger.OrderASC,
					},
					{
						Index: 1,
						Name:  "column2",
						Order: merger.OrderDESC,
					},
				}
				s, err := merger.NewSortColumns(columns...)
				require.NoError(t, err)
				return s
			},
			distinctCols: []merger.ColumnInfo{
				{Index: 0, Name: "column1"},
				{Index: 1, Name: "column2"},
			},
			wantErr: nil,
		},
		{
			name: "应该返回merger_去重列不为空且不含重复列_排序列表是去重列的子集",
			sortColsFunc: func(t *testing.T) merger.SortColumns {
				t.Helper()
				columns := []merger.ColumnInfo{
					{
						Index: 1,
						Name:  "column2",
						Order: merger.OrderDESC,
					},
				}
				s, err := merger.NewSortColumns(columns...)
				require.NoError(t, err)
				return s
			},
			distinctCols: []merger.ColumnInfo{
				{Index: 0, Name: "column1"},
				{Index: 1, Name: "column2"},
			},
			wantErr: nil,
		},
		{
			name: "应该返回merger_去重列不为空且不含重复列_排序列为空",
			sortColsFunc: func(t *testing.T) merger.SortColumns {
				t.Helper()
				return merger.SortColumns{}
			},
			distinctCols: []merger.ColumnInfo{
				{Index: 0, Name: "column1"},
				{Index: 1, Name: "column2"},
			},
			wantErr: nil,
		},
		{
			name: "应该返回错误_去重列表有重复列",
			sortColsFunc: func(t *testing.T) merger.SortColumns {
				t.Helper()
				columns := []merger.ColumnInfo{
					{
						Name:  "column1",
						Order: merger.OrderASC,
					},
					{
						Name:  "column2",
						Order: merger.OrderDESC,
					},
				}
				s, err := merger.NewSortColumns(columns...)
				require.NoError(t, err)
				return s
			},
			distinctCols: []merger.ColumnInfo{
				{Index: 0, Name: "column1"},
				{Index: 0, Name: "column1"},
			},
			wantErr: errs.ErrDistinctColsRepeated,
		},
		{
			name: "应该返回错误_去重列表为空",
			sortColsFunc: func(t *testing.T) merger.SortColumns {
				t.Helper()
				columns := []merger.ColumnInfo{
					{
						Index: 0,
						Name:  "column1",
						Order: merger.OrderASC,
					},
					{
						Index: 1,
						Name:  "column2",
						Order: merger.OrderDESC,
					},
				}
				s, err := merger.NewSortColumns(columns...)
				require.NoError(t, err)
				return s
			},
			distinctCols: []merger.ColumnInfo{},
			wantErr:      errs.ErrDistinctColsIsNull,
		},
		{
			name: "应该返回错误_排序列表包含不在去重列表中的列",
			sortColsFunc: func(t *testing.T) merger.SortColumns {
				t.Helper()
				columns := []merger.ColumnInfo{
					{
						Index: 1,
						Name:  "column2",
						Order: merger.OrderDESC,
					},
				}
				s, err := merger.NewSortColumns(columns...)
				require.NoError(t, err)
				return s
			},
			distinctCols: []merger.ColumnInfo{
				{
					Index: 0,
					Name:  "column1",
					Order: merger.OrderASC,
				},
			},
			wantErr: errs.ErrSortColListNotContainDistinctCol,
		},
	}

	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			m, err := NewMerger(tc.distinctCols, tc.sortColsFunc(t))
			assert.ErrorIs(t, err, tc.wantErr)
			if err != nil {
				return
			}
			require.NotNil(t, m)
		})
	}
}

type MergerSuite struct {
	suite.Suite
	mockDB01 *sql.DB
	mock01   sqlmock.Sqlmock
	mockDB02 *sql.DB
	mock02   sqlmock.Sqlmock
	mockDB03 *sql.DB
	mock03   sqlmock.Sqlmock
	mockDB04 *sql.DB
	mock04   sqlmock.Sqlmock
}

func (ms *MergerSuite) SetupTest() {
	t := ms.T()
	ms.initMock(t)
}

func (ms *MergerSuite) TearDownTest() {
	_ = ms.mockDB01.Close()
	_ = ms.mockDB02.Close()
	_ = ms.mockDB03.Close()
	_ = ms.mockDB04.Close()
}

func (ms *MergerSuite) initMock(t *testing.T) {
	var err error
	ms.mockDB01, ms.mock01, err = sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	if err != nil {
		t.Fatal(err)
	}
	ms.mockDB02, ms.mock02, err = sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	if err != nil {
		t.Fatal(err)
	}
	ms.mockDB03, ms.mock03, err = sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	if err != nil {
		t.Fatal(err)
	}
	ms.mockDB04, ms.mock04, err = sqlmock.New(sqlmock.QueryMatcherOption(sqlmock.QueryMatcherEqual))
	if err != nil {
		t.Fatal(err)
	}
}

func (ms *MergerSuite) TestMerger_Merge() {
	testcases := []struct {
		name    string
		merger  func() (*Merger, error)
		ctx     func() (context.Context, context.CancelFunc)
		wantErr error
		sqlRows func() []rows.Rows
	}{
		{
			name: "sqlRows字段不同",
			merger: func() (*Merger, error) {
				sortCols, err := merger.NewSortColumns(merger.ColumnInfo{
					Name:  "id",
					Order: merger.OrderASC,
				})
				require.NoError(ms.T(), err)
				return NewMerger([]merger.ColumnInfo{
					{Index: 0, Name: "id"},
					{Index: 2, Name: "name"},
					{Index: 3, Name: "address"},
				}, sortCols)
			},
			ctx: func() (context.Context, context.CancelFunc) {
				return context.WithCancel(context.Background())
			},
			sqlRows: func() []rows.Rows {
				query := "SELECT * FROM `t1`"
				ms.mock01.ExpectQuery(query).WillReturnRows(sqlmock.NewRows([]string{"id", "name", "address"}).AddRow(1, "abel", "cn").AddRow(5, "bruce", "cn"))
				ms.mock02.ExpectQuery(query).WillReturnRows(sqlmock.NewRows([]string{"id", "name", "email"}).AddRow(3, "alex", "cn").AddRow(4, "x", "cn"))
				dbs := []*sql.DB{ms.mockDB01, ms.mockDB02}
				rowsList := make([]rows.Rows, 0, len(dbs))
				for _, db := range dbs {
					row, err := db.QueryContext(context.Background(), query)
					require.NoError(ms.T(), err)
					rowsList = append(rowsList, row)
				}
				return rowsList
			},
			wantErr: errs.ErrDistinctColsNotInCols,
		},
		{
			name: "sqlRows字段不同_少一个字段",
			merger: func() (*Merger, error) {
				sortCols, err := merger.NewSortColumns(merger.ColumnInfo{
					Name:  "id",
					Order: merger.OrderASC,
				})
				require.NoError(ms.T(), err)
				return NewMerger([]merger.ColumnInfo{
					{Index: 0, Name: "id"},
					{Index: 2, Name: "name"},
					{Index: 3, Name: "address"},
				}, sortCols)

			},
			ctx: func() (context.Context, context.CancelFunc) {
				return context.WithCancel(context.Background())
			},
			sqlRows: func() []rows.Rows {
				query := "SELECT * FROM `t1`"
				ms.mock01.ExpectQuery(query).WillReturnRows(sqlmock.NewRows([]string{"id", "name", "address"}).AddRow(1, "abel", "cn").AddRow(5, "bruce", "cn"))
				ms.mock02.ExpectQuery(query).WillReturnRows(sqlmock.NewRows([]string{"id", "name"}).AddRow(3, "alex").AddRow(4, "x"))
				dbs := []*sql.DB{ms.mockDB01, ms.mockDB02}
				rowsList := make([]rows.Rows, 0, len(dbs))
				for _, db := range dbs {
					row, err := db.QueryContext(context.Background(), query)
					require.NoError(ms.T(), err)
					rowsList = append(rowsList, row)
				}
				return rowsList
			},
			wantErr: errs.ErrDistinctColsNotInCols,
		},
		{
			name: "sqlRows列表为空",
			ctx: func() (context.Context, context.CancelFunc) {
				return context.WithCancel(context.Background())
			},
			merger: func() (*Merger, error) {
				sortCols, err := merger.NewSortColumns(merger.ColumnInfo{
					Name:  "id",
					Order: merger.OrderASC,
				})
				require.NoError(ms.T(), err)
				return NewMerger([]merger.ColumnInfo{
					{Index: 0, Name: "id"},
					{Index: 2, Name: "name"},
					{Index: 3, Name: "address"},
				}, sortCols)

			},
			sqlRows: func() []rows.Rows {
				return []rows.Rows{}
			},
			wantErr: errs.ErrMergerEmptyRows,
		},
		{
			name: "sqlRows列表有nil",
			merger: func() (*Merger, error) {
				sortCols, err := merger.NewSortColumns(merger.ColumnInfo{
					Name:  "id",
					Order: merger.OrderASC,
				})
				require.NoError(ms.T(), err)
				return NewMerger([]merger.ColumnInfo{
					{Index: 0, Name: "id"},
					{Index: 2, Name: "name"},
					{Index: 3, Name: "address"},
				}, sortCols)
			},
			ctx: func() (context.Context, context.CancelFunc) {
				return context.WithCancel(context.Background())
			},
			sqlRows: func() []rows.Rows {
				return []rows.Rows{nil}
			},
			wantErr: errs.ErrMergerRowsIsNull,
		},
		{
			name: "数据库中的列不包含distinct的列",
			merger: func() (*Merger, error) {
				sortCols, err := merger.NewSortColumns(merger.ColumnInfo{
					Name:  "id",
					Order: merger.OrderASC,
				})
				require.NoError(ms.T(), err)
				return NewMerger([]merger.ColumnInfo{
					{Index: 0, Name: "id"},
					{Index: 2, Name: "name"},
					{Index: 3, Name: "address"},
				}, sortCols)
			},
			ctx: func() (context.Context, context.CancelFunc) {
				return context.WithCancel(context.Background())
			},
			sqlRows: func() []rows.Rows {
				query := "SELECT * FROM `t1`"
				ms.mock01.ExpectQuery(query).WillReturnRows(sqlmock.NewRows([]string{"id", "name", "email"}).AddRow(1, "abel", "cn").AddRow(5, "bruce", "cn"))
				ms.mock02.ExpectQuery(query).WillReturnRows(sqlmock.NewRows([]string{"id", "name", "address"}).AddRow(3, "alex", "cn").AddRow(4, "x", "cn"))
				dbs := []*sql.DB{ms.mockDB01, ms.mockDB02}
				rowsList := make([]rows.Rows, 0, len(dbs))
				for _, db := range dbs {
					row, err := db.QueryContext(context.Background(), query)
					require.NoError(ms.T(), err)
					rowsList = append(rowsList, row)
				}
				return rowsList
			},
			wantErr: errs.ErrDistinctColsNotInCols,
		},
		{
			name: "数据库中的列顺序和distinct的列顺序不一致",
			merger: func() (*Merger, error) {
				sortCols, err := merger.NewSortColumns(merger.ColumnInfo{
					Name:  "id",
					Order: merger.OrderASC,
				})
				require.NoError(ms.T(), err)
				return NewMerger([]merger.ColumnInfo{
					{Index: 0, Name: "id"},
					{Index: 1, Name: "name"},
					{Index: 2, Name: "address"},
				}, sortCols)
			},
			ctx: func() (context.Context, context.CancelFunc) {
				return context.WithCancel(context.Background())
			},
			sqlRows: func() []rows.Rows {
				query := "SELECT * FROM `t1`"
				ms.mock01.ExpectQuery(query).WillReturnRows(sqlmock.NewRows([]string{"id", "email", "name"}).AddRow(1, "abel", "cn").AddRow(5, "bruce", "cn"))
				ms.mock02.ExpectQuery(query).WillReturnRows(sqlmock.NewRows([]string{"id", "name", "email"}).AddRow(3, "alex", "cn").AddRow(4, "x", "cn"))
				dbs := []*sql.DB{ms.mockDB01, ms.mockDB02}
				rowsList := make([]rows.Rows, 0, len(dbs))
				for _, db := range dbs {
					row, err := db.QueryContext(context.Background(), query)
					require.NoError(ms.T(), err)
					rowsList = append(rowsList, row)
				}
				return rowsList
			},
			wantErr: errs.ErrDistinctColsNotInCols,
		},
	}

	for _, tc := range testcases {
		ms.T().Run(tc.name, func(t *testing.T) {
			m, err := tc.merger()
			require.NoError(ms.T(), err)
			ctx, cancel := tc.ctx()
			r, err := m.Merge(ctx, tc.sqlRows())
			cancel()
			assert.Equal(t, tc.wantErr, err)
			if err != nil {
				return
			}
			require.NotNil(t, r)
		})
	}
}

func (ms *MergerSuite) TestRows_NextAndScan() {
	testcases := []struct {
		name            string
		sqlRows         func() []rows.Rows
		wantVal         []TestModel
		sortColumnsFunc func(t *testing.T) merger.SortColumns
		distinctColumns []merger.ColumnInfo
		wantErr         error
	}{
		{
			name: "所有的列全部相同_排序列表是去重列表的子集",
			sqlRows: func() []rows.Rows {
				cols := []string{"id", "name", "address"}
				query := "SELECT * FROM `t1`"
				ms.mock01.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow(1, "abel", "cn"))
				ms.mock02.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow(1, "abel", "cn"))
				ms.mock03.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow(1, "abel", "cn"))
				dbs := []*sql.DB{ms.mockDB01, ms.mockDB02, ms.mockDB03}
				rowsList := make([]rows.Rows, 0, len(dbs))
				for _, db := range dbs {
					row, err := db.QueryContext(context.Background(), query)
					require.NoError(ms.T(), err)
					rowsList = append(rowsList, row)
				}
				return rowsList
			},
			wantVal: []TestModel{
				{
					Id:      1,
					Name:    "abel",
					Address: "cn",
				},
			},
			sortColumnsFunc: func(t *testing.T) merger.SortColumns {
				t.Helper()
				cols, err := merger.NewSortColumns(
					merger.ColumnInfo{
						Name:  "id",
						Order: merger.OrderDESC,
					},
				)
				require.NoError(t, err)
				return cols
			},
			distinctColumns: []merger.ColumnInfo{
				{
					Index: 0, Name: "id",
				},
				{
					Index: 1, Name: "name",
				},
				{
					Index: 2, Name: "address",
				},
			},
		},
		{
			name: "所有的列全部相同_排序列表与去重列表相同",
			sqlRows: func() []rows.Rows {
				cols := []string{"id", "name", "address"}
				query := "SELECT * FROM `t1`"
				ms.mock01.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow(1, "abel", "cn").AddRow(2, "alex", "cn"))
				ms.mock02.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow(1, "abel", "cn").AddRow(2, "alex", "cn"))
				ms.mock03.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow(1, "abel", "cn").AddRow(2, "alex", "cn"))
				dbs := []*sql.DB{ms.mockDB01, ms.mockDB02, ms.mockDB03}
				rowsList := make([]rows.Rows, 0, len(dbs))
				for _, db := range dbs {
					row, err := db.QueryContext(context.Background(), query)
					require.NoError(ms.T(), err)
					rowsList = append(rowsList, row)
				}
				return rowsList
			},
			wantVal: []TestModel{
				{
					Id:      1,
					Name:    "abel",
					Address: "cn",
				},
				{
					Id:      2,
					Name:    "alex",
					Address: "cn",
				},
			},
			sortColumnsFunc: func(t *testing.T) merger.SortColumns {
				t.Helper()
				cols, err := merger.NewSortColumns(
					merger.ColumnInfo{
						Name:  "id",
						Order: merger.OrderDESC,
					},
					merger.ColumnInfo{
						Name:  "name",
						Order: merger.OrderDESC,
					},
					merger.ColumnInfo{
						Name:  "address",
						Order: merger.OrderDESC,
					},
				)
				require.NoError(t, err)
				return cols
			},
			distinctColumns: []merger.ColumnInfo{
				{
					Index: 0, Name: "id",
				},
				{
					Index: 1, Name: "name",
				},
				{
					Index: 2, Name: "address",
				},
			},
		},
		{
			name: "部分列相同",
			sqlRows: func() []rows.Rows {
				cols := []string{"id", "name", "address"}
				query := "SELECT * FROM `t1`"
				ms.mock01.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow(1, "abel", "cn").AddRow(1, "abel", "kn"))
				ms.mock02.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow(1, "abel", "cn").AddRow(1, "alex", "cn"))
				ms.mock03.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow(2, "alex", "cn"))
				dbs := []*sql.DB{ms.mockDB01, ms.mockDB02, ms.mockDB03}
				rowsList := make([]rows.Rows, 0, len(dbs))
				for _, db := range dbs {
					row, err := db.QueryContext(context.Background(), query)
					require.NoError(ms.T(), err)
					rowsList = append(rowsList, row)
				}
				return rowsList
			},
			sortColumnsFunc: func(t *testing.T) merger.SortColumns {
				t.Helper()
				cols, err := merger.NewSortColumns(merger.ColumnInfo{
					Name:  "id",
					Order: merger.OrderDESC,
				})
				require.NoError(t, err)
				return cols
			},
			distinctColumns: []merger.ColumnInfo{
				{
					Index: 0, Name: "id",
				},
				{
					Index: 1, Name: "name",
				},
				{
					Index: 2, Name: "address",
				},
			},
			wantVal: []TestModel{
				{2, "alex", "cn"},
				{1, "abel", "cn"},
				{1, "abel", "kn"},
				{1, "alex", "cn"},
			},
		},
		{
			name: "有多个顺序列相同的情况",
			sortColumnsFunc: func(t *testing.T) merger.SortColumns {
				t.Helper()
				cols, err := merger.NewSortColumns(merger.ColumnInfo{
					Name:  "id",
					Order: merger.OrderASC,
				})
				require.NoError(t, err)
				return cols
			},
			distinctColumns: []merger.ColumnInfo{
				{
					Index: 0,
					Name:  "id",
				},
				{
					Index: 1,
					Name:  "name",
				},
				{
					Index: 2,
					Name:  "address",
				},
			},
			sqlRows: func() []rows.Rows {
				cols := []string{"id", "name", "address"}
				query := "SELECT * FROM `t1`"
				ms.mock01.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow(1, "abel", "cn").AddRow(1, "abel", "kn").AddRow(2, "alex", "cn"))
				ms.mock02.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow(1, "abel", "cn").AddRow(1, "alex", "cn").AddRow(2, "alex", "kn"))
				ms.mock03.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow(2, "alex", "cn").AddRow(2, "alex", "kn").AddRow(3, "alex", "cn"))
				dbs := []*sql.DB{ms.mockDB01, ms.mockDB02, ms.mockDB03}
				rowsList := make([]rows.Rows, 0, len(dbs))
				for _, db := range dbs {
					row, err := db.QueryContext(context.Background(), query)
					require.NoError(ms.T(), err)
					rowsList = append(rowsList, row)
				}
				return rowsList
			},
			wantVal: []TestModel{
				{1, "abel", "cn"},
				{1, "abel", "kn"},
				{1, "alex", "cn"},
				{2, "alex", "cn"},
				{2, "alex", "kn"},
				{3, "alex", "cn"},
			},
		},
		{
			name: "多个排序列，Order by id name,distinct id name address",
			sortColumnsFunc: func(t *testing.T) merger.SortColumns {
				t.Helper()
				cols, err := merger.NewSortColumns(merger.ColumnInfo{
					Name:  "id",
					Order: merger.OrderASC,
				}, merger.ColumnInfo{
					Name:  "name",
					Order: merger.OrderDESC,
				})
				require.NoError(t, err)
				return cols
			},
			distinctColumns: []merger.ColumnInfo{
				{
					Index: 0,
					Name:  "id",
				},
				{
					Index: 1,
					Name:  "name",
				},
				{
					Index: 2,
					Name:  "address",
				},
			},
			sqlRows: func() []rows.Rows {
				cols := []string{"id", "name", "address"}
				query := "SELECT * FROM `t1`"
				ms.mock01.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow(1, "abel", "cn").AddRow(1, "abel", "kn").AddRow(2, "alex", "cn"))
				ms.mock02.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow(1, "alex", "cn").AddRow(1, "abel", "cn").AddRow(2, "alex", "kn"))
				ms.mock03.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow(2, "alex", "cn").AddRow(2, "alex", "kn").AddRow(3, "alex", "cn"))
				dbs := []*sql.DB{ms.mockDB01, ms.mockDB02, ms.mockDB03}
				rowsList := make([]rows.Rows, 0, len(dbs))
				for _, db := range dbs {
					row, err := db.QueryContext(context.Background(), query)
					require.NoError(ms.T(), err)
					rowsList = append(rowsList, row)
				}
				return rowsList
			},
			wantVal: []TestModel{
				{1, "alex", "cn"},
				{1, "abel", "cn"},
				{1, "abel", "kn"},
				{2, "alex", "cn"},
				{2, "alex", "kn"},
				{3, "alex", "cn"},
			},
		},
		{
			name: "多个排序列，Order by id address,distinct id name address",
			sortColumnsFunc: func(t *testing.T) merger.SortColumns {
				t.Helper()
				cols, err := merger.NewSortColumns(merger.ColumnInfo{
					Index: 0,
					Name:  "id",
					Order: merger.OrderASC,
				}, merger.ColumnInfo{
					Index: 2,
					Name:  "address",
					Order: merger.OrderASC,
				})
				require.NoError(t, err)
				return cols
			},
			distinctColumns: []merger.ColumnInfo{
				{
					Index: 0,
					Name:  "id",
				},
				{
					Index: 1,
					Name:  "name",
				},
				{
					Index: 2,
					Name:  "address",
				},
			},
			sqlRows: func() []rows.Rows {
				cols := []string{"id", "name", "address"}
				query := "SELECT * FROM `t1`"
				ms.mock01.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow(1, "abel", "cn").AddRow(1, "abel", "kn").AddRow(2, "alex", "cn"))
				ms.mock02.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow(1, "alex", "cn").AddRow(1, "abel", "cn").AddRow(2, "alex", "kn"))
				ms.mock03.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow(2, "alex", "cn").AddRow(2, "alex", "kn").AddRow(3, "alex", "cn"))
				dbs := []*sql.DB{ms.mockDB01, ms.mockDB02, ms.mockDB03}
				rowsList := make([]rows.Rows, 0, len(dbs))
				for _, db := range dbs {
					row, err := db.QueryContext(context.Background(), query)
					require.NoError(ms.T(), err)
					rowsList = append(rowsList, row)
				}
				return rowsList
			},
			wantVal: []TestModel{
				{1, "abel", "cn"},
				{1, "alex", "cn"},
				{1, "abel", "kn"},
				{2, "alex", "cn"},
				{2, "alex", "kn"},
				{3, "alex", "cn"},
			},
		},
		{
			name: "Order by name, distinct id name address",
			sortColumnsFunc: func(t *testing.T) merger.SortColumns {
				t.Helper()
				cols, err := merger.NewSortColumns(merger.ColumnInfo{
					Name:  "name",
					Order: merger.OrderASC,
				})
				require.NoError(t, err)
				return cols
			},
			distinctColumns: []merger.ColumnInfo{
				{
					Index: 0,
					Name:  "id",
				},
				{
					Index: 1,
					Name:  "name",
				},
				{
					Index: 2,
					Name:  "address",
				},
			},
			sqlRows: func() []rows.Rows {
				cols := []string{"id", "name", "address"}
				query := "SELECT * FROM `t1`"
				ms.mock01.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow(1, "abel", "cn").AddRow(1, "abel", "kn").AddRow(2, "alex", "cn"))
				ms.mock02.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow(1, "abel", "cn").AddRow(1, "alex", "cn").AddRow(2, "alex", "kn"))
				ms.mock03.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow(2, "alex", "cn").AddRow(2, "alex", "kn").AddRow(3, "alex", "cn"))
				dbs := []*sql.DB{ms.mockDB01, ms.mockDB02, ms.mockDB03}
				rowsList := make([]rows.Rows, 0, len(dbs))
				for _, db := range dbs {
					row, err := db.QueryContext(context.Background(), query)
					require.NoError(ms.T(), err)
					rowsList = append(rowsList, row)
				}
				return rowsList
			},
			wantVal: []TestModel{
				{1, "abel", "cn"},
				{1, "abel", "kn"},
				{1, "alex", "cn"},
				{2, "alex", "cn"},
				{2, "alex", "kn"},
				{3, "alex", "cn"},
			},
		},
	}
	for _, tc := range testcases {
		ms.T().Run(tc.name, func(t *testing.T) {
			m, err := NewMerger(tc.distinctColumns, tc.sortColumnsFunc(t))
			require.NoError(t, err)
			r, err := m.Merge(context.Background(), tc.sqlRows())
			require.NoError(t, err)
			ans := make([]TestModel, 0, len(tc.wantVal))
			for r.Next() {
				t := TestModel{}
				err = r.Scan(&t.Id, &t.Name, &t.Address)
				require.NoError(ms.T(), err)
				ans = append(ans, t)
			}
			assert.Equal(t, tc.wantVal, ans)
		})
	}
}

func (ms *MergerSuite) TestRows_NotHaveOrderBy() {
	testcases := []struct {
		name            string
		wantVal         []TestModel
		distinctColumns []merger.ColumnInfo
		wantErr         error
		sqlRows         func() []rows.Rows
	}{
		{
			distinctColumns: []merger.ColumnInfo{
				{
					Index: 0,
					Name:  "id",
				},
				{
					Index: 1,
					Name:  "name",
				},
				{
					Index: 2,
					Name:  "address",
				},
			},
			name: "去重未含orderby",
			sqlRows: func() []rows.Rows {
				cols := []string{"id", "name", "address"}
				query := "SELECT * FROM `t1`"
				ms.mock01.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow(1, "abel", "cn").AddRow(1, "abel", "k"+
					"n"))
				ms.mock02.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow(1, "abel", "cn").AddRow(1, "alex", "cn"))
				ms.mock03.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow(1, "alex", "cn").AddRow(2, "alex", "cn"))
				dbs := []*sql.DB{ms.mockDB01, ms.mockDB02, ms.mockDB03}
				rowsList := make([]rows.Rows, 0, len(dbs))
				for _, db := range dbs {
					row, err := db.QueryContext(context.Background(), query)
					require.NoError(ms.T(), err)
					rowsList = append(rowsList, row)
				}
				return rowsList
			},
			wantVal: []TestModel{
				{1, "abel", "cn"},
				{1, "abel", "kn"},
				{1, "alex", "cn"},
				{2, "alex", "cn"},
			},
		},
	}
	for _, tc := range testcases {
		ms.T().Run(tc.name, func(t *testing.T) {
			m, err := NewMerger(tc.distinctColumns, merger.SortColumns{})
			require.NoError(t, err)
			r, err := m.Merge(context.Background(), tc.sqlRows())
			require.NoError(t, err)
			ans := make([]TestModel, 0, len(tc.wantVal))
			for r.Next() {
				t := TestModel{}
				err = r.Scan(&t.Id, &t.Name, &t.Address)
				require.NoError(ms.T(), err)
				ans = append(ans, t)
			}
			assert.Equal(t, tc.wantVal, ans)
		})
	}
}

func (ms *MergerSuite) TestRows_NextAndErr() {
	testcases := []struct {
		name            string
		rowsListFunc    func(t *testing.T) []rows.Rows
		wantErr         error
		sortColumnsFunc func(t *testing.T) merger.SortColumns
		distinctColumns []merger.ColumnInfo
	}{
		{
			name: "sqlRows列表中有一个返回error",
			rowsListFunc: func(t *testing.T) []rows.Rows {
				t.Helper()
				cols := []string{"id", "name", "address"}
				query := "SELECT * FROM `t1`"
				ms.mock01.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow(3, "abel", "cn").AddRow(3, "abel", "kn").RowError(1, mockErr))
				ms.mock02.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow(3, "abel", "kn").AddRow(2, "alex", "cn"))
				ms.mock03.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow(2, "alex", "cn"))
				return getResultSet(t, query, ms.mockDB01, ms.mockDB02, ms.mockDB03)
			},
			sortColumnsFunc: func(t *testing.T) merger.SortColumns {
				t.Helper()
				cols, err := merger.NewSortColumns(
					merger.ColumnInfo{
						Name:  "id",
						Order: merger.OrderDESC,
					},
					merger.ColumnInfo{
						Name:  "name",
						Order: merger.OrderASC,
					},
					merger.ColumnInfo{
						Name:  "address",
						Order: merger.OrderDESC,
					})
				require.NoError(t, err)
				return cols
			},
			distinctColumns: []merger.ColumnInfo{
				{
					Index: 0, Name: "id",
				},
				{
					Index: 1, Name: "name",
				},
				{
					Index: 2, Name: "address",
				},
			},
			wantErr: mockErr,
		},
	}
	for _, tc := range testcases {
		ms.T().Run(tc.name, func(t *testing.T) {
			m, err := NewMerger(tc.distinctColumns, tc.sortColumnsFunc(t))
			require.NoError(t, err)
			r, err := m.Merge(context.Background(), tc.rowsListFunc(t))
			require.NoError(t, err)
			for r.Next() {
			}
			assert.Equal(t, tc.wantErr, r.Err())
		})
	}
}

func (ms *MergerSuite) TestRows_Columns() {
	t := ms.T()
	// t.Skip()
	cols := []string{"id", "name", "address"}
	query := "SELECT * FROM `t1`"
	ms.mock01.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow(1, "abel", "cn").AddRow(1, "abel", "kn"))
	ms.mock02.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow(1, "abel", "cn").AddRow(1, "alex", "cn"))
	ms.mock03.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow(2, "alex", "cn"))
	sortCols, err := merger.NewSortColumns(merger.ColumnInfo{
		Name:  "id",
		Order: merger.OrderDESC,
	})
	require.NoError(t, err)
	m, err := NewMerger([]merger.ColumnInfo{
		{
			Index: 0, Name: "id",
		},
		{
			Index: 1, Name: "name",
		},
		{
			Index: 2, Name: "address",
		},
	}, sortCols)
	require.NoError(ms.T(), err)
	dbs := []*sql.DB{ms.mockDB01, ms.mockDB02, ms.mockDB03}
	rowsList := make([]rows.Rows, 0, len(dbs))
	for _, db := range dbs {
		row, err := db.QueryContext(context.Background(), query)
		require.NoError(ms.T(), err)
		rowsList = append(rowsList, row)
	}
	r, err := m.Merge(context.Background(), rowsList)
	require.NoError(ms.T(), err)
	t.Run("Next没有迭代完", func(t *testing.T) {
		for r.Next() {
			columns, err := r.Columns()
			require.NoError(t, err)
			assert.Equal(t, cols, columns)
		}
		require.NoError(t, r.Err())
	})
	t.Run("Next迭代完", func(t *testing.T) {
		require.False(t, r.Next())
		require.NoError(t, r.Err())
		_, err := r.Columns()
		assert.Equal(t, errs.ErrMergerRowsClosed, err)
	})
}

func (ms *MergerSuite) TestRows_ColumnTypes() {
	t := ms.T()

	query := "SELECT DISTINCT `grade` FROM `t1`"
	cols := []string{"`grade`"}
	ms.mock01.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow(100))
	ms.mock02.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow(90))
	ms.mock03.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow(110))

	columns := []merger.ColumnInfo{
		{
			Name:     "`grade`",
			Distinct: true,
		},
	}
	m, err := NewMerger(columns, merger.SortColumns{})
	require.NoError(t, err)

	r, err := m.Merge(context.Background(), getResultSet(t, query, ms.mockDB01, ms.mockDB02, ms.mockDB03))
	require.NoError(t, err)

	t.Run("rows未关闭", func(t *testing.T) {
		types, err := r.ColumnTypes()
		require.NoError(t, err)

		names := make([]string, 0, len(types))
		for _, typ := range types {
			names = append(names, typ.Name())
		}
		require.Equal(t, []string{"`grade`"}, names)
	})

	t.Run("rows已关闭", func(t *testing.T) {
		require.NoError(t, r.Close())

		_, err = r.ColumnTypes()
		require.ErrorIs(t, err, errs.ErrMergerRowsClosed)
	})
}

func (ms *MergerSuite) TestRows_Close() {
	cols := []string{"id"}
	query := "SELECT * FROM `t1`"
	ms.mock01.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow("1"))
	ms.mock02.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow("2").AddRow("5").AddRow("6").CloseError(newCloseMockErr("db02")))
	ms.mock03.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow("3").AddRow("5").AddRow("7").CloseError(newCloseMockErr("db03")))
	sortCols, err := merger.NewSortColumns(merger.ColumnInfo{
		Name:  "id",
		Order: merger.OrderDESC,
	})
	require.NoError(ms.T(), err)
	m, err := NewMerger([]merger.ColumnInfo{
		{
			Index: 0, Name: "id",
		},
	}, sortCols)
	require.NoError(ms.T(), err)
	dbs := []*sql.DB{ms.mockDB01, ms.mockDB02, ms.mockDB03}
	rowsList := make([]rows.Rows, 0, len(dbs))
	for _, db := range dbs {
		row, err := db.QueryContext(context.Background(), query)
		require.NoError(ms.T(), err)
		rowsList = append(rowsList, row)
	}
	r, err := m.Merge(context.Background(), rowsList)
	require.NoError(ms.T(), err)
	// 判断当前是可以正常读取的
	require.True(ms.T(), r.Next())
	var id int
	err = r.Scan(&id)
	require.NoError(ms.T(), err)
	err = r.Close()
	ms.T().Run("close返回multierror", func(t *testing.T) {
		assert.Equal(ms.T(), multierr.Combine(newCloseMockErr("db02"), newCloseMockErr("db03")), err)
	})
	ms.T().Run("close之后Next返回false", func(t *testing.T) {
		for i := 0; i < len(rowsList); i++ {
			require.False(ms.T(), rowsList[i].Next())
		}
		require.False(ms.T(), r.Next())
	})
	ms.T().Run("close之后Scan返回迭代过程中的错误", func(t *testing.T) {
		var id int
		err := r.Scan(&id)
		assert.Equal(t, errs.ErrMergerRowsClosed, err)
	})
	ms.T().Run("close之后调用Columns方法返回错误", func(t *testing.T) {
		_, err := r.Columns()
		require.Error(t, err)
	})
	ms.T().Run("close多次是等效的", func(t *testing.T) {
		for i := 0; i < 4; i++ {
			err = r.Close()
			require.NoError(t, err)
		}
	})
}

func (ms *MergerSuite) TestRows_Scan() {
	t := ms.T()
	t.Run("未调用Next，直接Scan，返回错", func(t *testing.T) {
		cols := []string{"id", "name", "address"}
		query := "SELECT * FROM `t1`"
		ms.mock01.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow(1, "abel", "cn").AddRow(5, "bruce", "cn"))
		r, err := ms.mockDB01.QueryContext(context.Background(), query)
		require.NoError(t, err)
		rowsList := []rows.Rows{r}
		sortCols, err := merger.NewSortColumns(merger.ColumnInfo{
			Name:  "id",
			Order: merger.OrderDESC,
		})
		require.NoError(t, err)
		m, err := NewMerger([]merger.ColumnInfo{
			{
				Index: 0, Name: "id",
			},
			{
				Index: 1, Name: "name",
			},
			{
				Index: 2, Name: "address",
			},
		}, sortCols)
		require.NoError(t, err)
		rr, err := m.Merge(context.Background(), rowsList)
		require.NoError(t, err)
		model := TestModel{}
		err = rr.Scan(&model.Id, &model.Name, &model.Address)
		assert.Equal(t, errs.ErrMergerScanNotNext, err)
	})
	t.Run("迭代过程中发现错误,调用Scan返回迭代中发现的错误", func(t *testing.T) {
		cols := []string{"id", "name", "address"}
		query := "SELECT * FROM `t1`"
		ms.mock01.ExpectQuery(query).WillReturnRows(sqlmock.NewRows(cols).AddRow(7, "curry", "cn").AddRow(6, "bruce", "cn").AddRow(5, "alex", "cn").RowError(2, mockErr))
		rowsList := getResultSet(t, query, ms.mockDB01)
		sortCols, err := merger.NewSortColumns(
			merger.ColumnInfo{
				Name:  "id",
				Order: merger.OrderDESC,
			},
			merger.ColumnInfo{
				Name:  "name",
				Order: merger.OrderDESC,
			},
			merger.ColumnInfo{
				Name:  "address",
				Order: merger.OrderDESC,
			},
		)
		require.NoError(t, err)
		m, err := NewMerger([]merger.ColumnInfo{
			{
				Index: 0, Name: "id",
			},
			{
				Index: 1, Name: "name",
			},
			{
				Index: 2, Name: "address",
			},
		}, sortCols)
		require.NoError(t, err)
		rr, err := m.Merge(context.Background(), rowsList)
		require.NoError(t, err)
		for rr.Next() {
		}
		var model TestModel
		err = rr.Scan(&model.Id, &model.Name, &model.Address)
		assert.Equal(t, mockErr, err)
	})
}

func TestMerger(t *testing.T) {
	suite.Run(t, &MergerSuite{})
	suite.Run(t, &NullableMergerSuite{})
}

type NullableMergerSuite struct {
	suite.Suite
	db01 *sql.DB
	db02 *sql.DB
	db03 *sql.DB
}

func (ms *NullableMergerSuite) SetupSuite() {
	t := ms.T()
	query := "CREATE TABLE t1 (\n      id int primary key,\n      `age`  int,\n    \t`name` varchar(20)\n  );\n"
	db01, err := sql.Open("sqlite3", "file:test01.db?cache=shared&mode=memory")
	if err != nil {
		t.Fatal(err)
	}
	ms.db01 = db01
	_, err = db01.ExecContext(context.Background(), query)
	if err != nil {
		t.Fatal(err)
	}
	db02, err := sql.Open("sqlite3", "file:test02.db?cache=shared&mode=memory")
	if err != nil {
		t.Fatal(err)
	}
	ms.db02 = db02
	_, err = db02.ExecContext(context.Background(), query)
	if err != nil {
		t.Fatal(err)
	}
	db03, err := sql.Open("sqlite3", "file:test03.db?cache=shared&mode=memory")
	if err != nil {
		t.Fatal(err)
	}
	ms.db03 = db03
	_, err = db03.ExecContext(context.Background(), query)
	if err != nil {
		t.Fatal(err)
	}
}

func (ms *NullableMergerSuite) TearDownSuite() {
	_ = ms.db01.Close()
	_ = ms.db02.Close()
	_ = ms.db03.Close()
}

func (ms *NullableMergerSuite) TestRows_Nullable() {
	testcases := []struct {
		name         string
		rowsList     func() []rows.Rows
		sortColumns  []merger.ColumnInfo
		wantErr      error
		afterFunc    func()
		wantVal      []DistinctNullable
		DistinctCols []merger.ColumnInfo
	}{
		{
			name: "测试去重",
			rowsList: func() []rows.Rows {
				db1InsertSql := []string{
					"insert into `t1` (`id`, `name`) values (1,  'zwl')",
					"insert into `t1` (`id`, `age`, `name`) values (2, 10, 'zwl')",
					"insert into `t1` (`id`, `age`, `name`) values (3, 10, 'xz')",
					"insert into `t1` (`id`, `age`) values (4, 10)",
				}
				for _, s := range db1InsertSql {
					_, err := ms.db01.ExecContext(context.Background(), s)
					require.NoError(ms.T(), err)
				}
				db2InsertSql := []string{
					"insert into `t1` (`id`, `name`) values (5,  'zwl')",
					"insert into `t1` (`id`, `age`, `name`) values (6, 10, 'zwl')",
				}
				for _, s := range db2InsertSql {
					_, err := ms.db02.ExecContext(context.Background(), s)
					require.NoError(ms.T(), err)
				}
				db3InsertSql := []string{
					"insert into `t1` (`id`, `name`) values (7, 'zwl')",
					"insert into `t1` (`id`, `age`) values (8, 5)",
					"insert into `t1` (`id`, `age`,`name`) values (9, 10,'xz')",
				}
				for _, s := range db3InsertSql {
					_, err := ms.db03.ExecContext(context.Background(), s)
					require.NoError(ms.T(), err)
				}
				dbs := []*sql.DB{ms.db01, ms.db02, ms.db03}
				rowsList := make([]rows.Rows, 0, len(dbs))
				query := "SELECT DISTINCT `age`,`name` FROM `t1` ORDER BY `age`,`name` DESC"
				for _, db := range dbs {
					r, err := db.QueryContext(context.Background(), query)
					require.NoError(ms.T(), err)
					rowsList = append(rowsList, r)
				}
				return rowsList
			},
			sortColumns: []merger.ColumnInfo{
				{
					Name:  "age",
					Order: merger.OrderASC,
				},
			},
			DistinctCols: []merger.ColumnInfo{
				{
					Index: 0, Name: "age",
				},
				{
					Index: 1, Name: "name",
				},
			},
			afterFunc: func() {
				dbs := []*sql.DB{ms.db01, ms.db02, ms.db03}
				for _, db := range dbs {
					_, err := db.Exec("DELETE FROM `t1`;")
					require.NoError(ms.T(), err)
				}
			},
			wantVal: func() []DistinctNullable {
				return []DistinctNullable{
					{
						Age:  sql.NullInt64{Valid: false, Int64: 0},
						Name: sql.NullString{Valid: true, String: "zwl"},
					},
					{
						Age:  sql.NullInt64{Valid: true, Int64: 5},
						Name: sql.NullString{Valid: false, String: ""},
					},
					{
						Age:  sql.NullInt64{Valid: true, Int64: 10},
						Name: sql.NullString{Valid: false, String: ""},
					},
					{
						Age:  sql.NullInt64{Valid: true, Int64: 10},
						Name: sql.NullString{Valid: true, String: "xz"},
					},
					{
						Age:  sql.NullInt64{Valid: true, Int64: 10},
						Name: sql.NullString{Valid: true, String: "zwl"},
					},
				}
			}(),
		},
	}
	for _, tc := range testcases {
		ms.T().Run(tc.name, func(t *testing.T) {
			sortCols, err := merger.NewSortColumns(tc.sortColumns...)
			require.NoError(t, err)
			m, err := NewMerger(tc.DistinctCols, sortCols)
			require.NoError(t, err)
			r, err := m.Merge(context.Background(), tc.rowsList())
			require.NoError(t, err)
			res := make([]DistinctNullable, 0, len(tc.wantVal))
			for r.Next() {
				nullT := DistinctNullable{}
				err := r.Scan(&nullT.Age, &nullT.Name)
				require.NoError(ms.T(), err)
				res = append(res, nullT)
			}
			require.True(t, r.(*Rows).closed)
			assert.NoError(t, r.Err())
			assert.Equal(t, tc.wantVal, res)
			tc.afterFunc()
		})
	}
}

type DistinctNullable struct {
	Age  sql.NullInt64
	Name sql.NullString
}

type TestModel struct {
	Id      int
	Name    string
	Address string
}

func newCloseMockErr(dbName string) error {
	return fmt.Errorf("rows: %s MockCloseErr", dbName)
}

func TestRows_NextResultSet(t *testing.T) {
	assert.False(t, (&Rows{}).NextResultSet())
}
func getResultSet(t *testing.T, sql string, dbs ...*sql.DB) []rows.Rows {
	resultSet := make([]rows.Rows, 0, len(dbs))
	for _, db := range dbs {
		row, err := db.Query(sql)
		require.NoError(t, err)
		resultSet = append(resultSet, row)
	}
	return resultSet
}

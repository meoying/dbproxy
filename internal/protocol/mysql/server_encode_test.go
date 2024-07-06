//go:build e2e

package mysql

import (
	"context"
	"testing"
	"time"

	"github.com/magiconair/properties/assert"
	"github.com/stretchr/testify/require"
)

// 测试 MySQL 的整数的类型
// 包含三个用例：
// 1. 所有的字段都是一个随意取值
// 2. 所有的字段都是最小值
// 3. 所有的字段都是最大值
// 确保客户端收到的和服务端传递的是一样的。
func (s *ServerTestSuite) TestIntTypes() {
	testCases := []struct {
		name    string
		sql     string
		wantErr error
		wantRes []any
	}{
		{
			name: "随意整数",
			sql:  "SELECT * FROM test_int_type WHERE id = 1",
		},
		{
			name: "最大整数",
			sql:  "SELECT * FROM test_int_type WHERE id = 2",
		},
		{
			name: "最小整数",
			sql:  "SELECT * FROM test_int_type WHERE id = 3",
		},
		{
			name: "NULL值",
			sql:  "SELECT * FROM test_int_type WHERE id = 4",
		},
	}

	db, err := newDB()
	require.NoError(s.T(), err)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	for _, tc := range testCases {
		s.T().Run(tc.name, func(t *testing.T) {
			rows, err := db.QueryContext(ctx, tc.sql)
			require.NoError(s.T(), err)
			for rows.Next() {
				var id, typeTinyint, typeSmallint, typeMediumint, typeInt, typeInteger, typeBigint any
				err = rows.Scan(&id, &typeTinyint, &typeSmallint, &typeMediumint, &typeInt, &typeInteger, &typeBigint)
				require.NoError(s.T(), err)
				s.T().Log(id, typeTinyint, typeSmallint, typeMediumint, typeInt, typeInteger, typeBigint)
				res := []any{id, typeTinyint, typeSmallint, typeMediumint, typeInt, typeInteger, typeBigint}
				assert.Equal(t, res, tc.wantRes)
			}
		})
	}
}

// 测试 MySQL 的浮点的类型
// 确保客户端收到的和服务端传递的是一样的。
func (s *ServerTestSuite) TestFloatTypes() {
	testCases := []struct {
		name    string
		sql     string
		wantErr error
		wantRes []any
	}{
		{
			name: "随意浮点数",
			sql:  "SELECT * FROM test_float_type WHERE id = 1",
		},
		{
			name: "NULL值",
			sql:  "SELECT * FROM test_float_type WHERE id = 2",
		},
	}

	db, err := newDB()
	require.NoError(s.T(), err)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	for _, tc := range testCases {
		s.T().Run(tc.name, func(t *testing.T) {
			rows, err := db.QueryContext(ctx, tc.sql)
			require.NoError(s.T(), err)
			for rows.Next() {
				// 在这里读取并且打印数据
				// 这里需要用到指针给Scan，不然会报错
				var id, typeFloat, typeDouble, typeDecimal, typeNumeric, typeReal any
				err = rows.Scan(&id, &typeFloat, &typeDouble, &typeDecimal, &typeNumeric, &typeReal)
				require.NoError(s.T(), err)
				s.T().Log(id, typeFloat, typeDouble, typeDecimal, typeNumeric, typeReal)
				res := []any{id, typeFloat, typeDouble, typeDecimal, typeNumeric, typeReal}
				assert.Equal(t, res, tc.wantRes)
			}
		})
	}
}

// 测试 MySQL 的字符串的类型
// 确保客户端收到的和服务端传递的是一样的。
func (s *ServerTestSuite) TestStringTypes() {
	testCases := []struct {
		name    string
		sql     string
		wantErr error
		wantRes []any
	}{
		{
			name: "随意字符串",
			sql:  "SELECT * FROM test_string_type WHERE id = 1",
		},
		{
			name: "NULL值",
			sql:  "SELECT * FROM test_string_type WHERE id = 2",
		},
	}

	db, err := newDB()
	require.NoError(s.T(), err)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	for _, tc := range testCases {
		s.T().Run(tc.name, func(t *testing.T) {
			rows, err := db.QueryContext(ctx, tc.sql)
			require.NoError(s.T(), err)
			for rows.Next() {
				var id, typeChar, typeVarchar, typeTinytext, typeText, typeMediumtext, typeLongtext, typeEnum, typeSet, typeBinary, typeVarbinary, typeJson, typeBit any
				err = rows.Scan(&id, &typeChar, &typeVarchar, &typeTinytext, &typeText, &typeMediumtext, &typeLongtext, &typeEnum, &typeSet, &typeBinary, &typeVarbinary, &typeJson, &typeBit)
				require.NoError(s.T(), err)
				s.T().Log(id, typeChar, typeVarchar, typeTinytext, typeText, typeMediumtext, typeLongtext, typeEnum, typeSet, typeBinary, typeVarbinary, typeJson, typeBit)
				res := []any{id, typeChar, typeVarchar, typeTinytext, typeText, typeMediumtext, typeLongtext, typeEnum, typeSet, typeBinary, typeVarbinary, typeJson, typeBit}
				assert.Equal(t, res, tc.wantRes)
			}
		})
	}
}

// 测试 MySQL 的时间的类型
// 确保客户端收到的和服务端传递的是一样的。
func (s *ServerTestSuite) TestDateTypes() {
	testCases := []struct {
		name    string
		sql     string
		wantErr error
		wantRes []any
	}{
		{
			name: "随意日期",
			sql:  "SELECT * FROM test_date_type WHERE id = 1",
		},
		{
			name: "NULL值",
			sql:  "SELECT * FROM test_date_type WHERE id = 2",
		},
	}

	db, err := newDB()
	require.NoError(s.T(), err)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	for _, tc := range testCases {
		s.T().Run(tc.name, func(t *testing.T) {
			rows, err := db.QueryContext(ctx, tc.sql)
			require.NoError(s.T(), err)
			for rows.Next() {
				var id, typeDate, typeDatetime, typeTimestamp, typeTime, typeYear any
				err = rows.Scan(&id, &typeDate, &typeDatetime, &typeTimestamp, &typeTime, &typeYear)
				require.NoError(s.T(), err)
				s.T().Log(id, typeDate, typeDatetime, typeTimestamp, typeTime, typeYear)
				res := []any{id, typeDate, typeDatetime, typeTimestamp, typeTime, typeYear}
				assert.Equal(t, res, tc.wantRes)
			}
		})
	}
}

// 测试 MySQL 的地理位置的类型
// 确保客户端收到的和服务端传递的是一样的。
func (s *ServerTestSuite) TestGeographyTypes() {
	testCases := []struct {
		name    string
		sql     string
		wantErr error
		wantRes []any
	}{
		{
			name: "随意地理位置",
			sql:  "SELECT * FROM test_geography_type WHERE id = 1",
		},
		{
			name: "NULL值",
			sql:  "SELECT * FROM test_geography_type WHERE id = 2",
		},
	}

	db, err := newDB()
	require.NoError(s.T(), err)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	for _, tc := range testCases {
		s.T().Run(tc.name, func(t *testing.T) {
			rows, err := db.QueryContext(ctx, tc.sql)
			require.NoError(s.T(), err)
			for rows.Next() {
				var id, typeGeometry, typeGeomcollection, typeLinestring, typeMultilinestring, typePoint, typeMultipoint, typePolygon, typeMultipolygon any
				err = rows.Scan(&id, &typeGeometry, &typeGeomcollection, &typeLinestring, &typeMultilinestring, &typePoint, &typeMultipoint, &typePolygon, &typeMultipolygon)
				require.NoError(s.T(), err)
				s.T().Log(id, typeGeometry, typeGeomcollection, typeLinestring, typeMultilinestring, typePoint, typeMultipoint, typePolygon, typeMultipolygon)
				res := []any{id, typeGeometry, typeGeomcollection, typeLinestring, typeMultilinestring, typePoint, typeMultipoint, typePolygon, typeMultipolygon}
				assert.Equal(t, res, tc.wantRes)
			}
		})
	}
}

// 测试 MySQL 的地理位置的类型
// 确保客户端收到的和服务端传递的是一样的。
func (s *ServerTestSuite) TestFilePathTypes() {
	testCases := []struct {
		name    string
		sql     string
		wantErr error
		wantRes []any
	}{
		{
			name: "随意字符串",
			sql:  "SELECT * FROM test_file_path_type WHERE id = 1",
		},
		{
			name: "NULL值",
			sql:  "SELECT * FROM test_file_path_type WHERE id = 2",
		},
	}

	db, err := newDB()
	require.NoError(s.T(), err)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	for _, tc := range testCases {
		s.T().Run(tc.name, func(t *testing.T) {
			rows, err := db.QueryContext(ctx, tc.sql)
			require.NoError(s.T(), err)
			for rows.Next() {
				var id, typeTinyblob, typeMediumblob, typeBlob, typeLongblob any
				err = rows.Scan(&id, &typeTinyblob, &typeMediumblob, &typeBlob, &typeLongblob)
				require.NoError(t, err)
				t.Log(id, typeTinyblob, typeMediumblob, typeBlob, typeLongblob)
				res := []any{id, typeTinyblob, typeMediumblob, typeBlob, typeLongblob}
				assert.Equal(t, res, tc.wantRes)
			}
		})
	}
}

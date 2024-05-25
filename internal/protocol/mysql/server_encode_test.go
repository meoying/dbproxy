package mysql

import (
	"context"
	"database/sql"
	"github.com/magiconair/properties/assert"
	"github.com/stretchr/testify/require"
	"testing"
	"time"
)

// 测试 MySQL 的整数的类型
// 包含三个用例：
// 1. 所有的字段都是一个随意取值
// 2. 所有的字段都是最小值
// 3. 所有的字段都是最大值
// 确保客户端收到的和服务端传递的是一样的。
// TODO 问题点：我们返回的结果和服务端返回的结果不一样，也没有搞懂为什么有些字段能转成整数，有些却还是原来的[]byte
// got [1 [49] [50] [51] 4 5 [54]] want [1 1 2 3 4 5 6]
func (s *ServerTestSuite) TestIntTypes() {
	testCases := []struct {
		name    string
		sql     string
		wantErr error
		wantRes []any
	}{
		{
			name: "随意整数",
			sql:  "SELECT * FROM test_int_type where id = 1",
		},
		{
			name: "最大整数",
			sql:  "SELECT * FROM test_int_type where id = 2",
		},
		{
			name: "最小整数",
			sql:  "SELECT * FROM test_int_type where id = 3",
		},
	}

	db, err := sql.Open("mysql", "root:root@tcp(localhost:8306)/dbproxy")
	realDb, err1 := sql.Open("mysql", "root:root@tcp(localhost:13306)/dbproxy")
	require.NoError(s.T(), err)
	require.NoError(s.T(), err1)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	for _, tc := range testCases {
		s.T().Run(tc.name, func(t *testing.T) {
			rows, err := realDb.QueryContext(ctx, tc.sql)
			for rows.Next() {
				var id, typeTinyint, typeSmallint, typeMediumint, typeInt, typeInteger, typeBigint any
				err = rows.Scan(&id, &typeTinyint, &typeSmallint, &typeMediumint, &typeInt, &typeInteger, &typeBigint)
				require.NoError(s.T(), err)
				s.T().Log(id, typeTinyint, typeSmallint, typeMediumint, typeInt, typeInteger, typeBigint)
				tc.wantRes = []any{id, typeTinyint, typeSmallint, typeMediumint, typeInt, typeInteger, typeBigint}
			}
			rows, err = db.QueryContext(ctx, tc.sql)
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
// TODO 问题点：我们返回的结果和服务端返回的结果不一样，也没有搞懂为什么有些字段能转成整数，有些却还是原来的[]byte，但浮点型服务端也会返回[]byte
// got [1 [54 54 46 54 54] [57 57 57 46 57 57 57] [51 51 46 51 51] [49 50 51 52 53 54 46 55 56] [49 50 51 52 53 46 54 55 56 57]] want [1 66.66 999.999 [51 51 46 51 51] [49 50 51 52 53 54 46 55 56] 12345.6789]
func (s *ServerTestSuite) TestFloatTypes() {
	testCases := []struct {
		name    string
		sql     string
		wantErr error
		wantRes []any
	}{
		{
			name: "随意浮点数",
			sql:  "SELECT * FROM test_float_type where id = 1",
		},
	}

	db, err := sql.Open("mysql", "root:root@tcp(localhost:8306)/dbproxy")
	realDb, err1 := sql.Open("mysql", "root:root@tcp(localhost:13306)/dbproxy")
	require.NoError(s.T(), err)
	require.NoError(s.T(), err1)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	for _, tc := range testCases {
		s.T().Run(tc.name, func(t *testing.T) {
			rows, err := realDb.QueryContext(ctx, tc.sql)
			for rows.Next() {
				var id, typeFloat, typeDouble, typeDecimal, typeNumeric, typeReal any
				err = rows.Scan(&id, &typeFloat, &typeDouble, &typeDecimal, &typeNumeric, &typeReal)
				require.NoError(s.T(), err)
				s.T().Log(id, typeFloat, typeDouble, typeDecimal, typeNumeric, typeReal)
				tc.wantRes = []any{id, typeFloat, typeDouble, typeDecimal, typeNumeric, typeReal}
			}
			rows, err = db.QueryContext(ctx, tc.sql)
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
			sql:  "SELECT * FROM test_string_type where id = 1",
		},
	}

	db, err := sql.Open("mysql", "root:root@tcp(localhost:8306)/dbproxy")
	realDb, err1 := sql.Open("mysql", "root:root@tcp(localhost:13306)/dbproxy")
	require.NoError(s.T(), err)
	require.NoError(s.T(), err1)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	for _, tc := range testCases {
		s.T().Run(tc.name, func(t *testing.T) {
			rows, err := realDb.QueryContext(ctx, tc.sql)
			for rows.Next() {
				var id, typeChar, typeVarchar, typeTinytext, typeText, typeMediumtext, typeLongtext, typeEnum, typeSet, typeBinary, typeVarbinary, typeJson, typeBit any
				err = rows.Scan(&id, &typeChar, &typeVarchar, &typeTinytext, &typeText, &typeMediumtext, &typeLongtext, &typeEnum, &typeSet, &typeBinary, &typeVarbinary, &typeJson, &typeBit)
				require.NoError(s.T(), err)
				s.T().Log(id, typeChar, typeVarchar, typeTinytext, typeText, typeMediumtext, typeLongtext, typeEnum, typeSet, typeBinary, typeVarbinary, typeJson, typeBit)
				tc.wantRes = []any{id, typeChar, typeVarchar, typeTinytext, typeText, typeMediumtext, typeLongtext, typeEnum, typeSet, typeBinary, typeVarbinary, typeJson, typeBit}
			}
			rows, err = db.QueryContext(ctx, tc.sql)
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
			sql:  "SELECT * FROM test_date_type where id = 1",
		},
	}

	db, err := sql.Open("mysql", "root:root@tcp(localhost:8306)/dbproxy")
	realDb, err1 := sql.Open("mysql", "root:root@tcp(localhost:13306)/dbproxy")
	require.NoError(s.T(), err)
	require.NoError(s.T(), err1)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	for _, tc := range testCases {
		s.T().Run(tc.name, func(t *testing.T) {
			rows, err := realDb.QueryContext(ctx, tc.sql)
			for rows.Next() {
				var id, typeDate, typeDatetime, typeTimestamp, typeTime, type_year any
				err = rows.Scan(&id, &typeDate, &typeDatetime, &typeTimestamp, &typeTime, &type_year)
				require.NoError(s.T(), err)
				s.T().Log(id, typeDate, typeDatetime, typeTimestamp, typeTime, type_year)
				tc.wantRes = []any{id, typeDate, typeDatetime, typeTimestamp, typeTime, type_year}
			}
			rows, err = db.QueryContext(ctx, tc.sql)
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
			sql:  "SELECT * FROM test_geography_type where id = 1",
		},
	}

	db, err := sql.Open("mysql", "root:root@tcp(localhost:8306)/dbproxy")
	realDb, err1 := sql.Open("mysql", "root:root@tcp(localhost:13306)/dbproxy")
	require.NoError(s.T(), err)
	require.NoError(s.T(), err1)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	for _, tc := range testCases {
		s.T().Run(tc.name, func(t *testing.T) {
			rows, err := realDb.QueryContext(ctx, tc.sql)
			for rows.Next() {
				var id, typeGeometry, typeGeomcollection, typeLinestring, typeMultilinestring, typePoint, typeMultipoint, typePolygon, typeMultipolygon any
				err = rows.Scan(&id, &typeGeometry, &typeGeomcollection, &typeLinestring, &typeMultilinestring, &typePoint, &typeMultipoint, &typePolygon, &typeMultipolygon)
				require.NoError(s.T(), err)
				s.T().Log(id, typeGeometry, typeGeomcollection, typeLinestring, typeMultilinestring, typePoint, typeMultipoint, typePolygon, typeMultipolygon)
				tc.wantRes = []any{id, typeGeometry, typeGeomcollection, typeLinestring, typeMultilinestring, typePoint, typeMultipoint, typePolygon, typeMultipolygon}
			}
			rows, err = db.QueryContext(ctx, tc.sql)
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
			sql:  "SELECT * FROM test_file_path_type where id = 1",
		},
	}

	db, err := sql.Open("mysql", "root:root@tcp(localhost:8306)/dbproxy")
	realDb, err1 := sql.Open("mysql", "root:root@tcp(localhost:13306)/dbproxy")
	require.NoError(s.T(), err)
	require.NoError(s.T(), err1)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()
	for _, tc := range testCases {
		s.T().Run(tc.name, func(t *testing.T) {
			rows, err := realDb.QueryContext(ctx, tc.sql)
			for rows.Next() {
				var id, typeTinyblob, typeMediumblob, typeBlob, typeLongblob any
				err = rows.Scan(&id, &typeTinyblob, &typeMediumblob, &typeBlob, &typeLongblob)
				require.NoError(s.T(), err)
				s.T().Log(id, typeTinyblob, typeMediumblob, typeBlob, typeLongblob)
				tc.wantRes = []any{id, typeTinyblob, typeMediumblob, typeBlob, typeLongblob}
			}
			rows, err = db.QueryContext(ctx, tc.sql)
			require.NoError(s.T(), err)
			for rows.Next() {
				var id, typeTinyblob, typeMediumblob, typeBlob, typeLongblob any
				err = rows.Scan(&id, &typeTinyblob, &typeMediumblob, &typeBlob, &typeLongblob)
				require.NoError(s.T(), err)
				s.T().Log(id, typeTinyblob, typeMediumblob, typeBlob, typeLongblob)
				res := []any{id, typeTinyblob, typeMediumblob, typeBlob, typeLongblob}
				assert.Equal(t, res, tc.wantRes)
			}
		})
	}
}

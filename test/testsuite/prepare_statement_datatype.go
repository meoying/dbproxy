package testsuite

import (
	"context"
	"database/sql"
	"log"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

// PrepareStatementDataTypeTestSuite 用于验证网关形态下客户端与dbproxy之间对传输不同数据类型的MySQL协议的解析的正确性
type PrepareStatementDataTypeTestSuite struct {
	suite.Suite
	// 直连dbproxy代理的db
	proxyDB *sql.DB
	// 通过社区常用mysql-driver包直连mysql的db
	mysqlDB *sql.DB
}

func (s *PrepareStatementDataTypeTestSuite) SetProxyDBAndMySQLDB(proxyDB *sql.DB, mysqlDB *sql.DB) {
	s.proxyDB = proxyDB
	s.mysqlDB = mysqlDB
}

// TestIntTypes
// 测试 MySQL 的整数的类型
// 包含三个用例：
// 1. 所有的字段都是一个随意取值
// 2. 所有的字段都是最小值
// 3. 所有的字段都是最大值
// 确保客户端收到的和服务端传递的是一样的。
func (s *PrepareStatementDataTypeTestSuite) TestIntTypes() {
	t := s.T()
	testCases := []struct {
		name string
		sql  string
		args []any
	}{
		{
			name: "随意整数",
			sql:  s.getIntTypeQuery(),
			args: []any{1},
		},
		{
			name: "最大整数",
			sql:  s.getIntTypeQuery(),
			args: []any{2},
		},
		{
			name: "最小整数",
			sql:  s.getIntTypeQuery(),
			args: []any{3},
		},
		{
			name: "NULL值",
			sql:  s.getIntTypeQuery(),
			args: []any{4},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			expected := s.getValues(t, s.mysqlDB, tc.sql, tc.args, s.scanIntValues)
			log.Printf("expected = %#v\n", expected)
			actual := s.getValues(t, s.proxyDB, tc.sql, tc.args, s.scanIntValues)
			log.Printf("actual = %#v\n", actual)
			assert.Equal(t, expected, actual)
		})
	}
}

func (s *PrepareStatementDataTypeTestSuite) getIntTypeQuery() string {
	return "SELECT /*useMaster*/ `id`,`type_tinyint`, `type_smallint`,`type_mediumint`,`type_int`,`type_integer`,`type_bigint` FROM `test_int_type` WHERE `id` = ?"
}

type scanValuesFunc func(t *testing.T, rows *sql.Rows) [][]any

func (s *PrepareStatementDataTypeTestSuite) getValues(t *testing.T, db *sql.DB, sql string, args []any, scanValues scanValuesFunc) [][]any {
	t.Helper()
	stmt, err := db.PrepareContext(context.Background(), sql)
	require.NoError(t, err)

	rows, err := stmt.QueryContext(context.Background(), args...)
	require.NoError(t, err)

	columnTypes, err := rows.ColumnTypes()
	require.NoError(t, err)

	for _, columnType := range columnTypes {
		log.Printf("column Name = %s, ScanType = %s, DatabaseType = %s\n", columnType.Name(), columnType.ScanType().String(), columnType.DatabaseTypeName())
	}

	values := scanValues(t, rows)

	assert.NoError(t, rows.Close())
	assert.NoError(t, rows.Err())
	assert.NoError(t, stmt.Close())

	return values
}

func (s *PrepareStatementDataTypeTestSuite) scanIntValues(t *testing.T, rows *sql.Rows) [][]any {
	t.Helper()
	var values [][]any
	for rows.Next() {
		var id, typeTinyint, typeSmallint, typeMediumint, typeInt, typeInteger, typeBigint any
		err := rows.Scan(&id, &typeTinyint, &typeSmallint, &typeMediumint, &typeInt, &typeInteger, &typeBigint)
		require.NoError(t, err)

		t.Log(id, typeTinyint, typeSmallint, typeMediumint, typeInt, typeInteger, typeBigint)
		res := []any{id, typeTinyint, typeSmallint, typeMediumint, typeInt, typeInteger, typeBigint}
		values = append(values, res)
	}
	return values
}

// TestFloatTypes
// 测试 MySQL 的浮点的类型
// 确保客户端收到的和服务端传递的是一样的。
func (s *PrepareStatementDataTypeTestSuite) TestFloatTypes() {
	t := s.T()

	testCases := []struct {
		name string
		sql  string
		args []any
	}{
		{
			name: "随意浮点数",
			sql:  s.getFloatTypeQuery(),
			args: []any{1},
		},
		{
			name: "NULL值",
			sql:  s.getFloatTypeQuery(),
			args: []any{2},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			expected := s.getValues(t, s.mysqlDB, tc.sql, tc.args, s.scanFloatValues)
			actual := s.getValues(t, s.proxyDB, tc.sql, tc.args, s.scanFloatValues)
			require.Equal(t, expected, actual)
		})
	}
}

func (s *PrepareStatementDataTypeTestSuite) getFloatTypeQuery() string {
	return "SELECT /*useMaster*/ `id`,`type_float`, `type_double`,`type_decimal`,`type_numeric`,`type_real` FROM `test_float_type` WHERE `id` = ?"
}

func (s *PrepareStatementDataTypeTestSuite) scanFloatValues(t *testing.T, rows *sql.Rows) [][]any {
	var values [][]any
	for rows.Next() {
		var id, typeFloat, typeDouble, typeDecimal, typeNumeric, typeReal any
		err := rows.Scan(&id, &typeFloat, &typeDouble, &typeDecimal, &typeNumeric, &typeReal)
		require.NoError(t, err)
		t.Log(id, typeFloat, typeDouble, typeDecimal, typeNumeric, typeReal)
		res := []any{id, typeFloat, typeDouble, typeDecimal, typeNumeric, typeReal}
		values = append(values, res)
	}
	return values
}

// TestStringTypes
// 测试 MySQL 的字符串的类型
// 确保客户端收到的和服务端传递的是一样的。
func (s *PrepareStatementDataTypeTestSuite) TestStringTypes() {
	t := s.T()

	testCases := []struct {
		name string
		sql  string
		args []any
	}{
		{
			name: "随意字符串",
			sql:  s.getStringTypeQuery(),
			args: []any{1},
		},
		{
			name: "NULL值",
			sql:  s.getStringTypeQuery(),
			args: []any{2},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			expected := s.getValues(t, s.mysqlDB, tc.sql, tc.args, s.scanStringValues)
			actual := s.getValues(t, s.proxyDB, tc.sql, tc.args, s.scanStringValues)
			require.Equal(t, expected, actual)
		})
	}
}

func (s *PrepareStatementDataTypeTestSuite) getStringTypeQuery() string {
	return "SELECT /*useMaster*/ `id`,`type_char`, `type_varchar`, `type_tinytext`, `type_text`, `type_mediumtext`, `type_longtext`, `type_enum`, `type_set`, `type_binary`, `type_varbinary`, `type_json`, `type_bit` FROM `test_string_type` WHERE `id` = ?"
}

func (s *PrepareStatementDataTypeTestSuite) scanStringValues(t *testing.T, rows *sql.Rows) [][]any {
	var values [][]any
	for rows.Next() {
		var id, typeChar, typeVarchar, typeTinytext, typeText, typeMediumtext, typeLongtext, typeEnum, typeSet, typeBinary, typeVarbinary, typeJson, typeBit any
		err := rows.Scan(&id, &typeChar, &typeVarchar, &typeTinytext, &typeText, &typeMediumtext, &typeLongtext, &typeEnum, &typeSet, &typeBinary, &typeVarbinary, &typeJson, &typeBit)
		require.NoError(t, err)
		t.Log(id, typeChar, typeVarchar, typeTinytext, typeText, typeMediumtext, typeLongtext, typeEnum, typeSet, typeBinary, typeVarbinary, typeJson, typeBit)
		res := []any{id, typeChar, typeVarchar, typeTinytext, typeText, typeMediumtext, typeLongtext, typeEnum, typeSet, typeBinary, typeVarbinary, typeJson, typeBit}
		values = append(values, res)
	}
	return values
}

// TestDateTypes
// 测试 MySQL 的时间的类型
// 确保客户端收到的和服务端传递的是一样的。
func (s *PrepareStatementDataTypeTestSuite) TestDateTypes() {
	t := s.T()
	testCases := []struct {
		name string
		sql  string
		args []any
	}{
		{
			name: "随意日期",
			sql:  s.getDateTypeQuery(),
			args: []any{1},
		},
		{
			name: "NULL值",
			sql:  s.getDateTypeQuery(),
			args: []any{2},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			expected := s.getValues(t, s.mysqlDB, tc.sql, tc.args, s.scanDateValues)
			actual := s.getValues(t, s.proxyDB, tc.sql, tc.args, s.scanDateValues)
			require.Equal(t, expected, actual)
		})
	}
}

func (s *PrepareStatementDataTypeTestSuite) getDateTypeQuery() string {
	return "SELECT /*useMaster*/ `id`, `type_date`, `type_datetime`, `type_timestamp`, `type_time`, `type_year` FROM `test_date_type` WHERE `id` = ?"
}

func (s *PrepareStatementDataTypeTestSuite) scanDateValues(t *testing.T, rows *sql.Rows) [][]any {
	var values [][]any
	for rows.Next() {
		var id, typeDate, typeDatetime, typeTimestamp, typeTime, typeYear any
		err := rows.Scan(&id, &typeDate, &typeDatetime, &typeTimestamp, &typeTime, &typeYear)
		require.NoError(t, err)
		t.Log(id, typeDate, typeDatetime, typeTimestamp, typeTime, typeYear)
		res := []any{id, typeDate, typeDatetime, typeTimestamp, typeTime, typeYear}
		values = append(values, res)
	}
	return values
}

// TestGeographyTypes
// 测试 MySQL 的地理位置的类型
// 确保客户端收到的和服务端传递的是一样的。
func (s *PrepareStatementDataTypeTestSuite) TestGeographyTypes() {
	t := s.T()
	testCases := []struct {
		name string
		sql  string
		args []any
	}{
		{
			name: "随意地理位置",
			sql:  s.getGeographyTypeQuery(),
			args: []any{1},
		},
		{
			name: "NULL值",
			sql:  s.getGeographyTypeQuery(),
			args: []any{2},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			expected := s.getValues(t, s.mysqlDB, tc.sql, tc.args, s.scanGeographyValues)
			actual := s.getValues(t, s.proxyDB, tc.sql, tc.args, s.scanGeographyValues)
			require.Equal(t, expected, actual)
		})
	}
}

func (s *PrepareStatementDataTypeTestSuite) getGeographyTypeQuery() string {
	return "SELECT /*useMaster*/ `id`,`type_geometry`,`type_geomcollection`,`type_linestring`,`type_multilinestring`,`type_point`,`type_multipoint`,`type_polygon`,`type_multipolygon` FROM `test_geography_type` WHERE `id` = ?"
}

func (s *PrepareStatementDataTypeTestSuite) scanGeographyValues(t *testing.T, rows *sql.Rows) [][]any {
	var values [][]any
	for rows.Next() {
		var id, typeGeometry, typeGeometrycollection, typeLinestring, typeMultilinestring, typePoint, typeMultipoint, typePolygon, typeMultipolygon any
		err := rows.Scan(&id, &typeGeometry, &typeGeometrycollection, &typeLinestring, &typeMultilinestring, &typePoint, &typeMultipoint, &typePolygon, &typeMultipolygon)
		require.NoError(t, err)
		t.Log(id, typeGeometry, typeGeometrycollection, typeLinestring, typeMultilinestring, typePoint, typeMultipoint, typePolygon, typeMultipolygon)
		res := []any{id, typeGeometry, typeGeometrycollection, typeLinestring, typeMultilinestring, typePoint, typeMultipoint, typePolygon, typeMultipolygon}
		values = append(values, res)
	}
	return values
}

// TestFilePathTypes
// 测试 MySQL 的地理位置的类型
// 确保客户端收到的和服务端传递的是一样的。
func (s *PrepareStatementDataTypeTestSuite) TestFilePathTypes() {
	t := s.T()
	testCases := []struct {
		name string
		sql  string
		args []any
	}{
		{
			name: "随意字符串",
			sql:  s.getFilepathTypeQuery(),
			args: []any{1},
		},
		{
			name: "NULL值",
			sql:  s.getFilepathTypeQuery(),
			args: []any{2},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			expected := s.getValues(t, s.mysqlDB, tc.sql, tc.args, s.scanFilepathValues)
			actual := s.getValues(t, s.proxyDB, tc.sql, tc.args, s.scanFilepathValues)
			require.Equal(t, expected, actual)
		})
	}
}

func (s *PrepareStatementDataTypeTestSuite) getFilepathTypeQuery() string {
	return "SELECT /*useMaster*/ `id`,`type_tinyblob`,`type_mediumblob`,`type_blob`,`type_longblob` FROM `test_file_path_type` WHERE `id` = ?"
}

func (s *PrepareStatementDataTypeTestSuite) scanFilepathValues(t *testing.T, rows *sql.Rows) [][]any {
	var values [][]any
	for rows.Next() {
		var id, typeTinyblob, typeMediumblob, typeBlob, typeLongblob any
		err := rows.Scan(&id, &typeTinyblob, &typeMediumblob, &typeBlob, &typeLongblob)
		require.NoError(t, err)
		t.Log(id, typeTinyblob, typeMediumblob, typeBlob, typeLongblob)
		res := []any{id, typeTinyblob, typeMediumblob, typeBlob, typeLongblob}
		values = append(values, res)
	}
	return values
}

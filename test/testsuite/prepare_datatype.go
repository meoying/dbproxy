package testsuite

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
)

// PrepareDataTypeTestSuite 用于验证网关形态下客户端与dbproxy之间对传输不同数据类型的MySQL协议的解析的正确性
type PrepareDataTypeTestSuite struct {
	suite.Suite
	// 直连dbproxy代理的db
	proxyDB *sql.DB
	// 通过社区常用mysql-driver包直连mysql的db
	mysqlDB *sql.DB
}

func (s *PrepareDataTypeTestSuite) SetProxyDBAndMySQLDB(proxyDB *sql.DB, mysqlDB *sql.DB) {
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
func (s *PrepareDataTypeTestSuite) TestIntTypes() {
	t := s.T()
	testCases := []struct {
		name string
		sql  string
		args []any
	}{
		{
			name: "非NULL值_按照单个字段查询",
			sql:  s.generateSQL(s.intTypeQueryTmpl(), []string{`id`}, false),
			args: []any{1},
		},
		{
			name: "非NULL值_按照多个字段查询_最小值",
			sql:  s.generateSQL(s.intTypeQueryTmpl(), []string{`type_tinyint`, `type_smallint`, `type_mediumint`, `type_int`, `type_integer`, `type_bigint`}, false),
			args: []any{-128, -32768, -8388608, -2147483648, -2147483648, int64(-9223372036854775808)},
		},
		{
			name: "非NULL值_按照多个字段查询_中间值",
			sql:  s.generateSQL(s.intTypeQueryTmpl(), []string{`type_tinyint`, `type_smallint`, `type_mediumint`, `type_int`, `type_integer`, `type_bigint`}, false),
			args: []any{1, 2, 3, 4, 5, 6},
		},
		{
			name: "非NULL值_按照多个字段查询_最大值",
			sql:  s.generateSQL(s.intTypeQueryTmpl(), []string{`type_tinyint`, `type_smallint`, `type_mediumint`, `type_int`, `type_integer`, `type_bigint`}, false),
			args: []any{127, 32767, 8388607, 2147483647, 2147483647, int64(9223372036854775807)},
		},
		{
			name: "NULL值_按照单个字段查询",
			sql:  s.generateSQL(s.intTypeQueryTmpl(), []string{`id`}, false),
			args: []any{4},
		},
		{
			name: "NULL值_按照多个字段查询",
			sql:  s.generateSQL(s.intTypeQueryTmpl(), []string{`type_tinyint`, `type_smallint`, `type_mediumint`, `type_int`, `type_integer`, `type_bigint`}, true),
			args: []any{},
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

func (s *PrepareDataTypeTestSuite) intTypeQueryTmpl() string {
	return "SELECT /*useMaster*/ `id`,`type_tinyint`, `type_smallint`,`type_mediumint`,`type_int`,`type_integer`,`type_bigint` FROM `test_int_type` WHERE %s"
}

func (s *PrepareDataTypeTestSuite) generateSQL(tmpl string, columns []string, isNULL bool) string {
	for i := range columns {
		if isNULL {
			columns[i] = fmt.Sprintf("`%s` IS NULL", strings.Trim(columns[i], "`"))
		} else {
			columns[i] = fmt.Sprintf("`%s` = ?", strings.Trim(columns[i], "`"))
		}
	}
	return fmt.Sprintf(tmpl, strings.Join(columns, " AND "))
}

type scanValuesFunc func(t *testing.T, rows *sql.Rows) [][]any

func (s *PrepareDataTypeTestSuite) getValues(t *testing.T, db *sql.DB, sql string, args []any, scanValues scanValuesFunc) [][]any {
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

func (s *PrepareDataTypeTestSuite) scanIntValues(t *testing.T, rows *sql.Rows) [][]any {
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
func (s *PrepareDataTypeTestSuite) TestFloatTypes() {
	t := s.T()
	// t.Skip()
	testCases := []struct {
		name string
		sql  string
		args []any
	}{
		{
			name: "非NULL值_按单个字段查询_id",
			sql:  s.generateSQL(s.floatTypeQueryTmpl(), []string{`id`}, false),
			args: []any{1},
		},
		{
			name: "非NULL值_按单个字段查询_type_float_最小值",
			sql:  s.generateSQL(s.floatTypeQueryTmpl(), []string{`type_float`}, false),
			args: []any{-99999.99999},
		},
		{
			name: "非NULL值_按单个字段查询_type_double_最小值",
			sql:  s.generateSQL(s.floatTypeQueryTmpl(), []string{`type_double`}, false),
			args: []any{-99999.99999},
		},
		{
			name: "非NULL值_按单个字段查询_type_decimal_最小值",
			sql:  s.generateSQL(s.floatTypeQueryTmpl(), []string{`type_decimal`}, false),
			args: []any{-99999999.99},
		},
		{
			name: "非NULL值_按单个字段查询_type_numeric_最小值",
			sql:  s.generateSQL(s.floatTypeQueryTmpl(), []string{`type_numeric`}, false),
			args: []any{-99999999.99},
		},
		{
			name: "非NULL值_按单个字段查询_type_real_最小值",
			sql:  s.generateSQL(s.floatTypeQueryTmpl(), []string{`type_real`}, false),
			args: []any{-1.7976931348623158e+308},
		},
		{
			name: "NULL值_按多个字段查询_各个字段_中间值",
			sql:  s.generateSQL(s.floatTypeQueryTmpl(), []string{`type_float`, `type_double`, `type_decimal`, `type_numeric`, `type_real`}, false),
			args: []any{66.66000, 999.99900, 33.33, 123456.78, 12345.6789},
		},
		{
			name: "非NULL值_按单个字段查询_type_float_最大值",
			sql:  s.generateSQL(s.floatTypeQueryTmpl(), []string{`type_float`}, false),
			args: []any{99999.99999},
		},
		{
			name: "非NULL值_按单个字段查询_type_double_最大值",
			sql:  s.generateSQL(s.floatTypeQueryTmpl(), []string{`type_double`}, false),
			args: []any{99999.99999},
		},
		{
			name: "非NULL值_按单个字段查询_type_decimal_最大值",
			sql:  s.generateSQL(s.floatTypeQueryTmpl(), []string{`type_decimal`}, false),
			args: []any{99999999.99},
		},
		{
			name: "非NULL值_按单个字段查询_type_numeric_最大值",
			sql:  s.generateSQL(s.floatTypeQueryTmpl(), []string{`type_numeric`}, false),
			args: []any{99999999.99},
		},
		{
			name: "非NULL值_按单个字段查询_type_real_最大值",
			sql:  s.generateSQL(s.floatTypeQueryTmpl(), []string{`type_real`}, false),
			args: []any{1.7976931348623158e+308},
		},
		{
			name: "NULL值_按单个字段查询_id",
			sql:  s.generateSQL(s.floatTypeQueryTmpl(), []string{`id`}, false),
			args: []any{4},
		},
		{
			name: "NULL值_按多个字段查询",
			sql:  s.generateSQL(s.floatTypeQueryTmpl(), []string{`type_float`, `type_double`, `type_decimal`, `type_numeric`, `type_real`}, true),
			args: []any{},
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

func (s *PrepareDataTypeTestSuite) floatTypeQueryTmpl() string {
	return "SELECT /*useMaster*/ `id`,`type_float`, `type_double`,`type_decimal`,`type_numeric`,`type_real` FROM `test_float_type` WHERE %s"
}

func (s *PrepareDataTypeTestSuite) scanFloatValues(t *testing.T, rows *sql.Rows) [][]any {
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
func (s *PrepareDataTypeTestSuite) TestStringTypes() {
	t := s.T()
	testCases := []struct {
		name string
		sql  string
		args []any
	}{
		{
			name: "非NULL值_按单字段查询",
			sql:  s.generateSQL(s.stringTypeQueryTmpl(), []string{`id`}, false),
			args: []any{1},
		},
		{
			name: "非NULL值_按多个字段查询",
			sql:  s.generateSQL(s.stringTypeQueryTmpl(), []string{`type_char`, `type_varchar`, `type_tinytext`, `type_text`, `type_mediumtext`, `type_longtext`, `type_enum`, `type_set`, `type_binary`, `type_varbinary`, `type_json`, `type_bit`}, false),
			args: []any{"一", "二", "三", "四", "五", "六", "small", "b,c", "0x61626300000000000000", "abcdef", `{"age": 25, "name": "Tom", "address": {"city": "New York", "zipcode": "10001"}}`, "0010101010"},
		},
		{
			name: "NULL值_按单个字段查询",
			sql:  s.generateSQL(s.stringTypeQueryTmpl(), []string{`id`}, false),
			args: []any{2},
		},
		{
			name: "NULL值_按多个字段查询",
			sql:  s.generateSQL(s.stringTypeQueryTmpl(), []string{`type_char`, `type_varchar`, `type_tinytext`, `type_text`, `type_mediumtext`, `type_longtext`, `type_enum`, `type_set`, `type_binary`, `type_varbinary`, `type_json`, `type_bit`}, true),
			args: []any{},
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

func (s *PrepareDataTypeTestSuite) stringTypeQueryTmpl() string {
	return "SELECT /*useMaster*/ `id`,`type_char`, `type_varchar`, `type_tinytext`, `type_text`, `type_mediumtext`, `type_longtext`, `type_enum`, `type_set`, `type_binary`, `type_varbinary`, `type_json`, `type_bit` FROM `test_string_type` WHERE %s"
}

func (s *PrepareDataTypeTestSuite) scanStringValues(t *testing.T, rows *sql.Rows) [][]any {
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
func (s *PrepareDataTypeTestSuite) TestDateTypes() {
	t := s.T()
	testCases := []struct {
		name string
		sql  string
		args []any
	}{
		{
			name: "非NULL值_按单个字段查询",
			sql:  s.generateSQL(s.dateTypeQueryTmpl(), []string{`id`}, false),
			args: []any{1},
		},
		{
			name: "非NULL值_按多个字段查询",
			sql:  s.generateSQL(s.dateTypeQueryTmpl(), []string{`type_date`, `type_datetime`, `type_timestamp`, `type_time`, `type_year`}, false),
			args: []any{"2024-05-25", "2024-05-25 23:51:00", "2024-05-25 23:51:05", "23:51:08", "2024"},
		},
		{
			name: "NULL值_按单个字段查询",
			sql:  s.generateSQL(s.dateTypeQueryTmpl(), []string{`id`}, false),
			args: []any{2},
		},
		{
			name: "NULL值_按多个字段查询",
			sql:  s.generateSQL(s.dateTypeQueryTmpl(), []string{`type_date`, `type_datetime`, `type_timestamp`, `type_time`, `type_year`}, true),
			args: []any{},
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

func (s *PrepareDataTypeTestSuite) dateTypeQueryTmpl() string {
	return "SELECT /*useMaster*/ `id`, `type_date`, `type_datetime`, `type_timestamp`, `type_time`, `type_year` FROM `test_date_type` WHERE %s"
}

func (s *PrepareDataTypeTestSuite) scanDateValues(t *testing.T, rows *sql.Rows) [][]any {
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
func (s *PrepareDataTypeTestSuite) TestGeographyTypes() {
	t := s.T()
	testCases := []struct {
		name string
		sql  string
		args []any
	}{
		{
			name: "非NULL值_按单个字段查询",
			sql:  s.generateSQL(s.geographyTypeQueryTmpl(), []string{`id`}, false),
			args: []any{1},
		},
		{
			name: "非NULL值_按多个字段查询",
			sql:  s.generateSQL(s.geographyTypeQueryTmpl(), []string{`type_geometry`, `type_geomcollection`, `type_linestring`, `type_multilinestring`, `type_point`, `type_multipoint`, `type_polygon`, `type_multipolygon`}, false),
			args: []any{
				`0x0000000001020000000300000000000000000000000000000000000000000000000000F03F000000000000F03F00000000000000400000000000000040`,
				`0x000000000107000000020000000101000000000000000000F03F000000000000F03F01020000000300000000000000000000000000000000000000000000000000F03F000000000000F03F00000000000000400000000000000040`,
				`0x0000000001020000000300000000000000000000000000000000000000000000000000F03F000000000000F03F00000000000000400000000000000040`,
				`0x0000000001050000000200000001020000000300000000000000000000000000000000000000000000000000F03F000000000000F03F00000000000000400000000000000040010200000003000000000000000000004000000000000000400000000000000840000000000000084000000000000010400000000000001040`,
				`0x0000000001010000005E4BC8073D5B4440AAF1D24D628052C0`,
				`0x0000000001040000000200000001010000005E4BC8073D5B4440AAF1D24D628052C00101000000F46C567DAE0641404182E2C7988F5DC0`,
				`0x00000000010300000001000000050000000000000000000000000000000000000000000000000000000000000000002440000000000000244000000000000024400000000000002440000000000000000000000000000000000000000000000000`,
				`0x00000000010600000002000000010300000001000000050000000000000000000000000000000000000000000000000000000000000000002440000000000000244000000000000024400000000000002440000000000000000000000000000000000000000000000000010300000001000000050000000000000000003440000000000000344000000000000034400000000000003E400000000000003E400000000000003E400000000000003E40000000000000344000000000000034400000000000003440`,
			},
		},
		{
			name: "NULL值_按单个字段查询",
			sql:  s.generateSQL(s.geographyTypeQueryTmpl(), []string{`id`}, false),
			args: []any{2},
		},
		{
			name: "NULL值_按多个字段查询",
			sql:  s.generateSQL(s.geographyTypeQueryTmpl(), []string{`type_geometry`, `type_geomcollection`, `type_linestring`, `type_multilinestring`, `type_point`, `type_multipoint`, `type_polygon`, `type_multipolygon`}, true),
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

func (s *PrepareDataTypeTestSuite) geographyTypeQueryTmpl() string {
	return "SELECT /*useMaster*/ `id`,`type_geometry`,`type_geomcollection`,`type_linestring`,`type_multilinestring`,`type_point`,`type_multipoint`,`type_polygon`,`type_multipolygon` FROM `test_geography_type` WHERE %s"
}

func (s *PrepareDataTypeTestSuite) scanGeographyValues(t *testing.T, rows *sql.Rows) [][]any {
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
func (s *PrepareDataTypeTestSuite) TestFilePathTypes() {
	t := s.T()
	testCases := []struct {
		name string
		sql  string
		args []any
	}{
		{
			name: "非NULL值_按单个字段查询",
			sql:  s.generateSQL(s.filepathTypeQueryTmpl(), []string{`id`}, false),
			args: []any{1},
		},
		{
			name: "非NULL值_按多个字段查询",
			sql:  s.generateSQL(s.filepathTypeQueryTmpl(), []string{`type_tinyblob`, `type_mediumblob`, `type_blob`, `type_longblob`}, false),
			args: []any{`0x01020304FFFFFFFF0000000CAACB0000`, `0x01020304FFFFFFFF0000000CAACB0000`, `0x01020304FFFFFFFF0000000CAACB0000`, `0x01020304FFFFFFFF0000000CAACB0000`},
		},
		{
			name: "NULL值_按单个字段查询",
			sql:  s.generateSQL(s.filepathTypeQueryTmpl(), []string{`id`}, false),
			args: []any{2},
		},
		{
			name: "NULL值_按多个字段查询",
			sql:  s.generateSQL(s.filepathTypeQueryTmpl(), []string{`type_tinyblob`, `type_mediumblob`, `type_blob`, `type_longblob`}, true),
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

func (s *PrepareDataTypeTestSuite) filepathTypeQueryTmpl() string {
	return "SELECT /*useMaster*/ `id`,`type_tinyblob`,`type_mediumblob`,`type_blob`,`type_longblob` FROM `test_file_path_type` WHERE %s"
}

func (s *PrepareDataTypeTestSuite) scanFilepathValues(t *testing.T, rows *sql.Rows) [][]any {
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

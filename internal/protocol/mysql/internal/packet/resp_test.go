package packet

import (
	"bytes"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBuildStmtPrepareRespPacket(t *testing.T) {
	testcases := []struct {
		name       string
		stmtId     int
		countCol   int
		countParam int
		want       []byte
	}{
		{
			name:       "SELECT `order_id` FROM `order` WHERE `user_id` = ? ORDER BY `order_id`;",
			stmtId:     1,
			countCol:   1,
			countParam: 1,
			want:       []byte{0x00, 0x01, 0x00, 0x00, 0x00, 0x01, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00},
		},
	}
	for _, tc := range testcases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			actual := BuildStmtPrepareRespPacket(tc.stmtId, tc.countCol, tc.countParam)
			assert.Equal(t, actual[4:], tc.want)
		})
	}
}

func TestBuildBinaryResultsetRowRespPacket(t *testing.T) {
	tests := []struct {
		name     string
		values   []any
		expected []byte
	}{
		{
			name:   "Valid Row with Non-Null Fields",
			values: []any{int32(1), "Alice", int32(30)},
			expected: []byte{
				0x00,                   // packet header
				0x00,                   // null bitmap (no fields are NULL)
				0x01, 0x00, 0x00, 0x00, // id = 1 (int32, little endian)
				0x05,                    // length of 'Alice'
				'A', 'l', 'i', 'c', 'e', // name = 'Alice'
				0x1e, 0x00, 0x00, 0x00, // age = 30 (int32, little endian)
			},
		},
		{
			name:   "Valid Row with Null Field",
			values: []any{int8(2), nil, int64(25)},
			expected: []byte{
				0x00,                   // packet header
				0x08,                   // null bitmap (name is NULL)
				0x02,                   // id = 2 (int32, little endian)
				0x19, 0x00, 0x00, 0x00, // age = 25 (int64, little endian)
				0x00, 0x00, 0x00, 0x00, // age = 25 (int64, little endian)
			},
		},
		{
			name:   "Valid Row with Boolean Field",
			values: []any{int8(3), false, int16(40)},
			expected: []byte{
				0x00,       // packet header
				0x00,       // null bitmap (no fields are NULL)
				0x03,       // id = 3 (int8, little endian)
				0x00,       // boolean false
				0x28, 0x00, // age = 40 (int16, little endian)
			},
		},
		{
			name:   "Valid Row with Binary Data",
			values: []any{int64(4), []byte{0xDE, 0xAD}, int64(50)},
			expected: []byte{
				0x00,                   // packet header
				0x00,                   // null bitmap (no fields are NULL)
				0x04, 0x00, 0x00, 0x00, // id = 4 (int64, little endian)
				0x00, 0x00, 0x00, 0x00, // id = 4 (int64, little endian)
				0x02,       // length of binary data
				0xDE, 0xAD, // binary data
				0x32, 0x00, 0x00, 0x00, // age = 50 (int64, little endian)
				0x00, 0x00, 0x00, 0x00, // age = 50 (int64, little endian)
			},
		},
		{
			name:   "Valid Row with Various Types",
			values: []any{int32(1), "Alice", int16(30), int32(1000), int8(20), 10.5, true, []byte{0xBE, 0xEF}},
			expected: []byte{
				0x00,       // packet header
				0x00, 0x00, // null bitmap (2 byte long, no fields are NULL)
				0x01, 0x00, 0x00, 0x00, // id = 1 (int32, little endian)
				0x05,                    // length of 'Alice'
				'A', 'l', 'i', 'c', 'e', // name = 'Alice'
				0x1e, 0x00, // age = 30 (int16, little endian)
				0xe8, 0x03, 0x00, 0x00, // count = 1000 (int32, little endian)
				0x14,                   // small count = 20 (int8)
				0x00, 0x00, 0x00, 0x00, // value of 10.5 (float64, little endian)
				0x00, 0x00, 0x25, 0x40, // value of 10.5 (float64, little endian)
				0x01,       // boolean true
				0x02,       // length of binary data
				0xBE, 0xEF, // binary data
			},
		},
		{
			name:   "Row with Null Values",
			values: []any{int32(2), nil, int16(45), nil, int8(15)},
			expected: []byte{
				0x00,                   // packet header
				0x28,                   // null bitmap (1 byte long, second and fourth fields are NULL)
				0x02, 0x00, 0x00, 0x00, // id = 2 (int32, little endian)
				0x2d, 0x00, // age = 45 (int16, little endian)
				0x0f, // small count = 15 (int8)
			},
		},
		// {
		// 	name:   "Row with *[]byte",
		// 	values: []any{&[]uint8{0x31}, &[]uint8{0x31}, &[]uint8{0x32}, &[]uint8{0x33}, &[]uint8{0x34}, &[]uint8{0x34}, &[]uint8{0x36}},
		// 	expected: []byte{
		// 		0x00,                   // packet header
		// 		0x00,                   // null bitmap (1 byte long, second and fourth fields are NULL)
		// 		0x02, 0x00, 0x00, 0x00, // id = 2 (int32, little endian)
		// 		0x2d, 0x00, // age = 45 (int16, little endian)
		// 		0x0f, // small count = 15 (int8)
		// 	},
		// },
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			actual := BuildBinaryResultsetRowRespPacket(tc.values...)
			assert.Equal(t, tc.expected, actual[4:])
		})
	}
}

func TestConvertToBytes(t *testing.T) {
	const c3 = 3
	tests := []struct {
		name     string
		input    any
		expected []byte
	}{
		{
			input:    123,
			expected: []byte("123"),
		},
		{input: int8(12), expected: []byte("12")},
		{input: int16(1234), expected: []byte("1234")},
		{input: int32(12345), expected: []byte("12345")},
		{input: int64(123456), expected: []byte("123456")},
		{input: uint(123), expected: []byte("123")},
		{input: uint8(12), expected: []byte("12")},
		{input: uint16(1234), expected: []byte("1234")},
		{input: uint32(12345), expected: []byte("12345")},
		{input: uint64(123456), expected: []byte("123456")},
		{input: uintptr(123456), expected: []byte("123456")},
		{input: 45.67, expected: []byte("45.670000")},
		{input: float32(50.6), expected: []byte("50.599998")},
		{input: "hello world", expected: []byte("hello world")},
		{input: true, expected: []byte("true")},
		{input: false, expected: []byte("false")},
		{input: []byte{1, 2, 3}, expected: []byte{1, 2, 3}},
		{input: []int{1, 2, 3}, expected: []byte("[1 2 3]")},
		{input: [c3]int{1, 2, 3}, expected: []byte("[1 2 3]")},
		{input: map[string]int{"one": 1, "two": 2}, expected: []byte("map[one:1 two:2]")},
		{input: sql.NullInt64{Int64: 42, Valid: true}, expected: []byte("42")},
		{input: sql.NullInt64{Valid: false}, expected: nil},
		{input: sql.NullString{String: "hello", Valid: true}, expected: []byte("hello")},
		{input: sql.NullString{Valid: false}, expected: nil},
		{input: sql.NullFloat64{Float64: 45.67, Valid: true}, expected: []byte("45.670000")},
		{input: sql.NullFloat64{Valid: false}, expected: nil},
		{input: sql.NullBool{Bool: true, Valid: true}, expected: []byte("true")},
		{input: sql.NullBool{Valid: false}, expected: nil},
		{input: sql.NullTime{Time: time.Date(2022, time.January, 1, 12, 0, 0, 0, time.UTC), Valid: true}, expected: []byte("2022-01-01 12:00:00 +0000 UTC")},
		{input: sql.NullTime{Valid: false}, expected: nil},
		{input: time.Date(2022, time.January, 1, 12, 0, 0, 0, time.UTC), expected: []byte("2022-01-01 12:00:00 +0000 UTC")},
		// {input: time.Date(2022, time.January, 1, 12, 0, 0, 0, time.UTC), expected: []byte("2022-01-01 12:00:00 +0000 UTC")},
		{input: sql.NullByte{Byte: 1, Valid: true}, expected: []byte("1")},
		{input: sql.NullByte{Valid: false}, expected: nil},
		{input: sql.NullInt16{Int16: 1234, Valid: true}, expected: []byte("1234")},
		{input: sql.NullInt16{Valid: false}, expected: nil},
		{input: sql.NullInt32{Int32: 12345, Valid: true}, expected: []byte("12345")},
		{input: sql.NullInt32{Valid: false}, expected: nil},
		{input: complex64(1 + 2i), expected: []byte("(1+2i)")},
		{input: 2 + 3i, expected: []byte("(2+3i)")},
		{input: any(1), expected: []byte("1")},
		{input: nil, expected: nil},
		{input: any((*int64)(nil)), expected: nil},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(fmt.Sprintf("Converting %v", tc.input), func(t *testing.T) {
			output := convertToBytes(tc.input)
			assert.Equal(t, tc.expected, output)
		})
	}
}

func TestWriteBinaryValue(t *testing.T) {
	tests := []struct {
		name      string
		valueFunc func(t *testing.T) any
		expected  func(t *testing.T) []byte
	}{
		{
			name: "sql.NullInt64",
			valueFunc: func(t *testing.T) any {
				t.Helper()
				return sql.NullInt64{Int64: int64(2), Valid: true}
			},
			expected: func(t *testing.T) []byte {
				t.Helper()
				// var buf bytes.Buffer
				// assert.NoError(t, binary.Write(&buf, binary.LittleEndian, int64(2)))
				// return buf.Bytes()
				return []byte{0x2, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0}
			},
		},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			var buf bytes.Buffer
			err := writeBinaryValue(&buf, tc.valueFunc(t))
			assert.NoError(t, err)
			assert.Equal(t, tc.expected(t), buf.Bytes())
		})
	}
}

func TestConvertToMySQLBinaryProtocolValue(t *testing.T) {
	var nilBytes []byte
	tests := []struct {
		name       string
		getColumns func(t *testing.T) []*sqlmock.Column
		values     []any
		wantValues []any
	}{
		{
			name: "dbproxy.test_int_type_NULL值",
			getColumns: func(t *testing.T) []*sqlmock.Column {
				t.Helper()
				return []*sqlmock.Column{
					sqlmock.NewColumn("id").OfType("INT", ""),
					sqlmock.NewColumn("type_tinyint").OfType("TINYINT", ""),
					sqlmock.NewColumn("type_smallint").OfType("SMALLINT", ""),
					sqlmock.NewColumn("type_mediumint").OfType("MEDIUMINT", ""),
					sqlmock.NewColumn("type_int").OfType("INT", ""),
					sqlmock.NewColumn("type_int").OfType("INT", ""),
					sqlmock.NewColumn("type_bigint").OfType("BIGINT", ""),
				}
			},
			values: []any{
				&[]byte{0x34}, // '4'
				&nilBytes,
				&nilBytes,
				&nilBytes,
				&nilBytes,
				(*[]byte)(nil),
				nil,
			},
			wantValues: []any{
				int32(4),
				nil,
				nil,
				nil,
				nil,
				nil,
				nil,
			},
		},
		// {
		// 	name: "dbproxy.test_string_type",
		// 	getColumns: func(t *testing.T) []*sqlmock.Column {
		// 		t.Helper()
		// 		return []*sqlmock.Column{
		// 			sqlmock.NewColumn("id").OfType("INT", ""),
		// 			sqlmock.NewColumn("type_tinyint").OfType("TINYINT", ""),
		// 			sqlmock.NewColumn("type_smallint").OfType("SMALLINT", ""),
		// 			sqlmock.NewColumn("type_mediumint").OfType("MEDIUMINT", ""),
		// 			sqlmock.NewColumn("type_int").OfType("INT", ""),
		// 			sqlmock.NewColumn("type_int").OfType("INT", ""),
		// 			sqlmock.NewColumn("type_bigint").OfType("BIGINT", ""),
		// 		}
		// 	},
		// 	values: []any{
		// 		&[]byte{0x34}, // '4'
		// 		&nilBytes,
		// 		&nilBytes,
		// 		&nilBytes,
		// 		&nilBytes,
		// 		(*[]byte)(nil),
		// 		nil,
		// 	},
		// 	wantValues: []any{
		// 		int32(4),
		// 		nil,
		// 		nil,
		// 		nil,
		// 		nil,
		// 		nil,
		// 		nil,
		// 	},
		// },
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {

			db, mock, err := sqlmock.New()
			assert.NoError(t, err)

			mockRows := sqlmock.NewRowsWithColumnDefinition(tc.getColumns(t)...)

			expectedSQL := "SELECT *"
			mock.ExpectQuery(expectedSQL).WillReturnRows(mockRows)

			rows, err := db.Query(expectedSQL)
			require.NoError(t, err)

			cols, err := rows.ColumnTypes()
			require.NoError(t, err)
			require.NoError(t, rows.Close())

			require.Equal(t, len(tc.values), len(cols))

			gotValues := make([]any, len(tc.values))
			for i, val := range tc.values {
				gotValues[i], err = ConvertToBinaryProtocolValue(val, cols[i])
				assert.NoError(t, err)
			}

			assert.Equal(t, tc.wantValues, gotValues)
			assert.NoError(t, mock.ExpectationsWereMet())
		})
	}
}

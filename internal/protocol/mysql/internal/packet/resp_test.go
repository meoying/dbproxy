package packet

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

// func TestBuildStmtPrepareRespPacket(t *testing.T) {
// 	t.Skip()
// 	testcases := []struct {
// 		name       string
// 		stmtId     int
// 		countCol   int
// 		countParam int
// 		want       []byte
// 	}{
// 		{
// 			name:       "SELECT `order_id` FROM `order` WHERE `user_id` = ? ORDER BY `order_id`;",
// 			stmtId:     1,
// 			countCol:   1,
// 			countParam: 1,
// 			want:       []byte{0x00, 0x01, 0x00, 0x00, 0x00, 0x01, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00},
// 		},
// 	}
// 	for _, tc := range testcases {
// 		tc := tc
// 		t.Run(tc.name, func(t *testing.T) {
// 			actual := BuildStmtPrepareRespPacket(tc.stmtId, tc.countCol, tc.countParam)
// 			assert.Equal(t, actual[4:], tc.want)
// 		})
// 	}
// }

func TestBuildBinaryResultsetRowRespPacket(t *testing.T) {
	t.Skip()
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
			actual := BuildBinaryResultsetRowRespPacket(tc.values, nil)
			assert.Equal(t, tc.expected, actual[4:])
		})
	}
}

// func TestWriteBinaryValue(t *testing.T) {
// 	t.Skip()
// 	tests := []struct {
// 		name      string
// 		valueFunc func(t *testing.T) any
// 		expected  func(t *testing.T) []byte
// 	}{
// 		{
// 			name: "sql.NullInt64",
// 			valueFunc: func(t *testing.T) any {
// 				t.Helper()
// 				return sql.NullInt64{Int64: int64(2), Valid: true}
// 			},
// 			expected: func(t *testing.T) []byte {
// 				t.Helper()
// 				// var buf bytes.Buffer
// 				// assert.NoError(t, binary.Write(&buf, binary.LittleEndian, int64(2)))
// 				// return buf.Bytes()
// 				return []byte{0x2, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0, 0x0}
// 			},
// 		},
// 	}
//
// 	for _, tc := range tests {
// 		tc := tc
// 		t.Run(tc.name, func(t *testing.T) {
// 			var buf bytes.Buffer
// 			err := writeBinaryValue(&buf, tc.valueFunc(t))
// 			assert.NoError(t, err)
// 			assert.Equal(t, tc.expected(t), buf.Bytes())
// 		})
// 	}
// }

// func TestConvertToMySQLBinaryProtocolValue(t *testing.T) {
// 	var nilBytes []byte
// 	tests := []struct {
// 		name       string
// 		getColumns func(t *testing.T) []*sqlmock.Column
// 		values     []any
// 		wantValues []any
// 	}{
// 		{
// 			name: "dbproxy.test_int_type_NULL值",
// 			getColumns: func(t *testing.T) []*sqlmock.Column {
// 				t.Helper()
// 				return []*sqlmock.Column{
// 					sqlmock.NewColumn("id").OfType("INT", ""),
// 					sqlmock.NewColumn("type_tinyint").OfType("TINYINT", ""),
// 					sqlmock.NewColumn("type_smallint").OfType("SMALLINT", ""),
// 					sqlmock.NewColumn("type_mediumint").OfType("MEDIUMINT", ""),
// 					sqlmock.NewColumn("type_int").OfType("INT", ""),
// 					sqlmock.NewColumn("type_int").OfType("INT", ""),
// 					sqlmock.NewColumn("type_bigint").OfType("BIGINT", ""),
// 				}
// 			},
// 			values: []any{
// 				&[]byte{0x34}, // '4'
// 				&nilBytes,
// 				&nilBytes,
// 				&nilBytes,
// 				&nilBytes,
// 				(*[]byte)(nil),
// 				nil,
// 			},
// 			wantValues: []any{
// 				int32(4),
// 				nil,
// 				nil,
// 				nil,
// 				nil,
// 				nil,
// 				nil,
// 			},
// 		},
// 		// {
// 		// 	name: "dbproxy.test_string_type",
// 		// 	getColumns: func(t *testing.T) []*sqlmock.Column {
// 		// 		t.Helper()
// 		// 		return []*sqlmock.Column{
// 		// 			sqlmock.NewColumn("id").OfType("INT", ""),
// 		// 			sqlmock.NewColumn("type_tinyint").OfType("TINYINT", ""),
// 		// 			sqlmock.NewColumn("type_smallint").OfType("SMALLINT", ""),
// 		// 			sqlmock.NewColumn("type_mediumint").OfType("MEDIUMINT", ""),
// 		// 			sqlmock.NewColumn("type_int").OfType("INT", ""),
// 		// 			sqlmock.NewColumn("type_int").OfType("INT", ""),
// 		// 			sqlmock.NewColumn("type_bigint").OfType("BIGINT", ""),
// 		// 		}
// 		// 	},
// 		// 	values: []any{
// 		// 		&[]byte{0x34}, // '4'
// 		// 		&nilBytes,
// 		// 		&nilBytes,
// 		// 		&nilBytes,
// 		// 		&nilBytes,
// 		// 		(*[]byte)(nil),
// 		// 		nil,
// 		// 	},
// 		// 	wantValues: []any{
// 		// 		int32(4),
// 		// 		nil,
// 		// 		nil,
// 		// 		nil,
// 		// 		nil,
// 		// 		nil,
// 		// 		nil,
// 		// 	},
// 		// },
// 	}
//
// 	for _, tc := range tests {
// 		tc := tc
// 		t.Run(tc.name, func(t *testing.T) {
//
// 			db, mock, err := sqlmock.New()
// 			assert.NoError(t, err)
//
// 			mockRows := sqlmock.NewRowsWithColumnDefinition(tc.getColumns(t)...)
//
// 			expectedSQL := "SELECT *"
// 			mock.ExpectQuery(expectedSQL).WillReturnRows(mockRows)
//
// 			rows, err := db.Query(expectedSQL)
// 			require.NoError(t, err)
//
// 			cols, err := rows.ColumnTypes()
// 			require.NoError(t, err)
// 			require.NoError(t, rows.Close())
//
// 			require.Equal(t, len(tc.values), len(cols))
//
// 			gotValues := make([]any, len(tc.values))
// 			for i, val := range tc.values {
// 				gotValues[i], err = ConvertToBinaryProtocolValue(val, cols[i])
// 				assert.NoError(t, err)
// 			}
//
// 			assert.Equal(t, tc.wantValues, gotValues)
// 			assert.NoError(t, mock.ExpectationsWereMet())
// 		})
// 	}
// }

func TestParseTime(t *testing.T) {
	tests := []struct {
		name               string
		input              []byte
		columnDatabaseType string
		expected           string
		expectError        bool
	}{
		{
			name:               "Date Only",
			input:              []byte("2024-05-25"),
			columnDatabaseType: "DATE",
			expected:           "2024-05-25",
			expectError:        false,
		},
		{
			name:               "Date and Time without Seconds",
			input:              []byte("2024-05-25 23:51"),
			columnDatabaseType: "DATETIME",
			expected:           "2024-05-25 23:51",
			expectError:        false,
		},
		{
			name:               "Date and Time with Seconds",
			input:              []byte("2024-05-25 23:51:05"),
			columnDatabaseType: "TIMESTAMP",
			expected:           "2024-05-25 23:51:05",
			expectError:        false,
		},
		{
			name:               "Time Only",
			input:              []byte("23:51:08"),
			columnDatabaseType: "TIME",
			expected:           "23:51:08",
			expectError:        false,
		},
		{
			name:               "Invalid Date",
			input:              []byte("invalid-date"),
			columnDatabaseType: "DATE",
			expected:           "",
			expectError:        true,
		},
		{
			name:               "Unsupported Column Type",
			input:              []byte("2024-05-25"),
			columnDatabaseType: "UNSUPPORTED",
			expected:           "",
			expectError:        true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual, layout, err := parseTime(tt.input, tt.columnDatabaseType)
			if tt.expectError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, actual.Format(layout))
			}
		})
	}
}

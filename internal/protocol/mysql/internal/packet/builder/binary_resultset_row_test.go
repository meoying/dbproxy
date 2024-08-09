package builder

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBinaryResultsetRowPacket_Build(t *testing.T) {
	t.Skip()
	tests := []struct {
		name          string
		values        []any
		expected      []byte
		assertErrFunc assert.ErrorAssertionFunc
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
			assertErrFunc: assert.NoError,
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
			assertErrFunc: assert.NoError,
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
			assertErrFunc: assert.NoError,
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
			assertErrFunc: assert.NoError,
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
			assertErrFunc: assert.NoError,
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
			assertErrFunc: assert.NoError,
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
			b := BinaryResultSetRowPacket{values: tc.values, cols: nil}
			actual, err := b.Build()
			tc.assertErrFunc(t, err)
			if err != nil {
				return
			}
			assert.Equal(t, tc.expected, actual[4:])
		})
	}
}

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

package packet

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestExecuteStmtRequest_Parse(t *testing.T) {
	t.Skip()
	tests := []struct {
		name          string
		payload       []byte
		numParams     uint64
		expected      ExecuteStmtRequest
		expectErrFunc require.ErrorAssertionFunc
	}{
		{
			name:          "payload命令字段非法",
			payload:       []byte{0x16, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
			expected:      ExecuteStmtRequest{},
			expectErrFunc: require.Error,
		},
		{
			name: "Basic Execute Statement",
			payload: []byte{
				0x17,                   // command
				0x01, 0x00, 0x00, 0x00, // statement_id
				0x00,                   // flags
				0x01, 0x00, 0x00, 0x00, // iteration_count
			},
			numParams: uint64(1),
			expected: ExecuteStmtRequest{
				StatementID:    1,
				Flags:          0,
				IterationCount: 1,
			},
			expectErrFunc: require.NoError,
		},
		{
			name: "Execute Statement with Parameters",
			payload: []byte{
				0x17,                   // status
				0x01, 0x00, 0x00, 0x00, // statement_id
				0x00,                   // flags
				0x01, 0x00, 0x00, 0x00, // iteration_count
				0x01,       // parameter_count
				0x00,       // null_bitmap
				0x01,       // new_params_bind_flag
				0x0f, 0x00, // parameter_type
				0x03, 0x66, 0x6f, 0x6f, // parameter_name "foo"
			},
			numParams: uint64(1),
			expected: ExecuteStmtRequest{
				StatementID:       1,
				Flags:             0,
				IterationCount:    1,
				ParameterCount:    1,
				NullBitmap:        []byte{0x00},
				NewParamsBindFlag: 0x01,
				Parameters: []ExecuteStmtRequestParameter{
					{Type: 15, Name: "foo"},
				},
			},
			expectErrFunc: require.NoError,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var req ExecuteStmtRequest
			tt.expectErrFunc(t, req.Parse(tt.numParams, tt.payload))
			assert.Equal(t, tt.expected, req)
		})
	}
}

func TestParseExecuteStmtRequest(t *testing.T) {
	tests := []struct {
		name      string
		payload   []byte
		numParams uint64
		expected  ExecuteStmtRequest
		err       error
	}{
		// {
		// 	name: "Valid packet with parameters and CLIENT_QUERY_ATTRIBUTES不起作用",
		// 	payload: func() []byte {
		// 		var buf bytes.Buffer
		// 		binary.Write(&buf, binary.LittleEndian, byte(0x17)) // command
		// 		binary.Write(&buf, binary.LittleEndian, uint32(1))  // statement_id
		// 		binary.Write(&buf, binary.LittleEndian, byte(0x00)) // flags
		// 		binary.Write(&buf, binary.LittleEndian, uint32(1))  // iteration_count
		// 		// binary.Write(&buf, binary.LittleEndian, uint64(1))                 // parameter_count
		// 		buf.Write([]byte{0x00})                                  // null_bitmap
		// 		binary.Write(&buf, binary.LittleEndian, byte(1))         // new_params_bind_flag
		// 		binary.Write(&buf, binary.LittleEndian, MySQLTypeString) // parameter_type
		// 		// binary.Write(&buf, binary.LittleEndian, uint64(len("foo")))        // parameter_name length
		// 		// buf.Write([]byte("foo"))                                           // parameter_name
		// 		binary.Write(&buf, binary.LittleEndian, uint64(len("hello world"))) // parameter_value length
		// 		buf.Write([]byte("hello world"))                                    // parameter_value
		// 		return buf.Bytes()
		// 	}(),
		// 	numParams: 1,
		// 	expected: ExecuteStmtRequest{
		// 		Command:           0x17,
		// 		StatementID:       1,
		// 		Flags:             0x00,
		// 		IterationCount:    1,
		// 		ParameterCount:    1,
		// 		NullBitmap:        []byte{0x00},
		// 		NewParamsBindFlag: 0x01,
		// 		Parameters: []ExecuteStmtRequestParameter{
		// 			{
		// 				Type:  MySQLTypeString,
		// 				Value: "hello world",
		// 			},
		// 		},
		// 	},
		// 	err: nil,
		// },
		{
			name:      "Malformed payload",
			payload:   []byte{0x17, 0x01, 0x00},
			numParams: 1,
			expected:  ExecuteStmtRequest{},
			err:       fmt.Errorf("error reading StatementID: unexpected EOF"),
		},
		{
			name: "Valid packet without parameters",
			payload: func() []byte {
				var buf bytes.Buffer
				_ = binary.Write(&buf, binary.LittleEndian, byte(0x17)) // command
				_ = binary.Write(&buf, binary.LittleEndian, uint32(1))  // statement_id
				_ = binary.Write(&buf, binary.LittleEndian, byte(0x00)) // flags
				_ = binary.Write(&buf, binary.LittleEndian, uint32(1))  // iteration_count

				return buf.Bytes()
			}(),
			numParams: 0,
			expected: ExecuteStmtRequest{
				Command:           0x17,
				StatementID:       1,
				Flags:             0x00,
				IterationCount:    1,
				ParameterCount:    0,
				NullBitmap:        nil,
				NewParamsBindFlag: 0x00,
				Parameters:        nil,
			},
			err: nil,
		},
		{
			name:      "Invalid Command",
			payload:   []byte{0x18, 0x01, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01}, // Command is not 0x17
			numParams: 1,
			expected:  ExecuteStmtRequest{},
			err:       fmt.Errorf("invalid Command: 18"),
		},
		{
			name: "order",
			payload: []byte{
				// command
				0x17,
				// statement_id
				0x01, 0x00, 0x00, 0x00,
				// flags
				0x00,
				// iteration_count
				0x01, 0x00, 0x00, 0x00,
				// null_bitmap
				0x00,
				// new_params_bind_flag
				0x01,
				// parameter_type 第一个字节表示:FIELD_TYPE_LONGLONG (8), 第二个字节表示 unsigned Unsigned: 0
				0x08,
				0x00,
				// Value (INT64): 1002
				0xea, 0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
			},
			numParams: 1,
			expected: ExecuteStmtRequest{
				Command:           0x17,
				StatementID:       1,
				Flags:             0x00,
				IterationCount:    1,
				ParameterCount:    uint64(1),
				NullBitmap:        []byte{0x00},
				NewParamsBindFlag: 0x01,
				Parameters: []ExecuteStmtRequestParameter{
					{
						Type:  0x08,
						Value: uint64(1002),
					},
				},
			},
			err: nil,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var req ExecuteStmtRequest
			err := req.Parse(tt.numParams, tt.payload)
			if tt.err != nil {
				assert.EqualError(t, err, tt.err.Error())
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, req)
			}
		})
	}
}

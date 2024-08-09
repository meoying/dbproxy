package parser

import (
	"testing"

	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/flags"
	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/packet"
	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/packet/encoding"
	"github.com/stretchr/testify/assert"
)

func TestStmtExecutePacket_Parse(t *testing.T) {
	tests := []struct {
		name                  string
		payload               []byte
		numParams             uint64
		clientCapabilityFlags flags.CapabilityFlags
		expected              *StmtExecutePacket
		errAssertFunc         assert.ErrorAssertionFunc
	}{
		{
			name: "无参数",
			payload: func() []byte {
				return []byte{
					0x17,                   // command
					0x01, 0x00, 0x00, 0x00, // statement_id
					0x00,                   // flags
					0x01, 0x00, 0x00, 0x00, // iteration_count
				}
			}(),
			numParams: 0,
			expected: func() *StmtExecutePacket {
				p := NewStmtExecutePacket(0, 0)
				p.status = 0x17
				p.statementID = 1
				p.iterationCount = 1
				return p
			}(),
			errAssertFunc: assert.NoError,
		},
		{
			name: "单个参数",
			payload: []byte{
				0x17,                   // command
				0x01, 0x00, 0x00, 0x00, // statement_id
				0x00,                   // flags
				0x01, 0x00, 0x00, 0x00, // iteration_count
				0x00,       // null_bitmap
				0x01,       // new_params_bind_flag
				0x08, 0x00, // parameter_type 第一个字节表示:FIELD_TYPE_LONGLONG (8), 第二个字节表示 unsigned Unsigned: 0
				0xea, 0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, // Value (INT64): 1002
			},
			numParams: 1,
			expected: func() *StmtExecutePacket {
				p := NewStmtExecutePacket(0, 1)
				p.status = 0x17
				p.statementID = 1
				p.iterationCount = 1
				p.nullBitmap = []byte{0x00}
				p.newParamsBindFlag = 0x01
				p.parameterCount = 1
				p.parameters = []StmtExecuteParameter{
					{
						Type:  packet.MySQLTypeLongLong,
						Value: int64(1002),
					},
				}
				return p
			}(),
			errAssertFunc: assert.NoError,
		},
		{
			name: "单个参数_负数",
			payload: []byte{
				0x17,                   // command
				0x01, 0x00, 0x00, 0x00, // statement_id
				0x00,                   // flags
				0x01, 0x00, 0x00, 0x00, // iteration_count
				0x00,       // null_bitmap
				0x01,       // new_params_bind_flag
				0x08, 0x00, // parameter_type 第一个字节表示:FIELD_TYPE_LONGLONG (8), 第二个字节表示 unsigned Unsigned: 0
				0x80, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, // Value (INT64): -128
			},
			numParams: 1,
			expected: func() *StmtExecutePacket {
				p := NewStmtExecutePacket(0, 1)
				p.status = 0x17
				p.statementID = 1
				p.iterationCount = 1
				p.nullBitmap = []byte{0x00}
				p.newParamsBindFlag = 0x01
				p.parameterCount = 1
				p.parameters = []StmtExecuteParameter{
					{
						Type:  packet.MySQLTypeLongLong,
						Value: int64(-128),
					},
				}
				return p
			}(),
			errAssertFunc: assert.NoError,
		},
		{
			name: "单个参数_varchar类型",
			payload: []byte{
				0x17,                   // status
				0x01, 0x00, 0x00, 0x00, // statement_id
				0x00,                   // flags
				0x01, 0x00, 0x00, 0x00, // iteration_count
				0x00,       // null_bitmap
				0x01,       // new_params_bind_flag
				0x0f, 0x00, // parameter_type
				0x03, 0x66, 0x6f, 0x6f, // parameter_name "foo"
			},
			numParams: uint64(1),
			expected: func() *StmtExecutePacket {
				p := NewStmtExecutePacket(0, 1)
				p.status = 0x17
				p.statementID = 1
				p.iterationCount = 1
				p.parameterCount = 1
				p.nullBitmap = []byte{0x00}
				p.newParamsBindFlag = 0x01
				p.parameters = []StmtExecuteParameter{
					{Type: packet.MySQLTypeVarchar, Value: "foo"},
				}
				return p
			}(),
			errAssertFunc: assert.NoError,
		},
		{
			name: "单个参数_string类型_包含参数名_CLIENT_QUERY_ATTRIBUTES设置",
			payload: func() []byte {
				b := []byte{
					0x17,                   // command
					0x01, 0x00, 0x00, 0x00, // statement_id
					0x00,                   // flags
					0x01, 0x00, 0x00, 0x00, // iteration_count
					0x01,       // parameter_count
					0x00,       // null_bitmap
					0x01,       // new_params_bind_flag
					0xfe, 0x00, // parameter_type
				}
				// parameter_name
				b = append(b, encoding.LengthEncodeString("foo")...)
				// parameter_value
				b = append(b, encoding.LengthEncodeString("bar")...)
				return b
			}(),
			numParams:             1,
			clientCapabilityFlags: flags.ClientQueryAttributes,
			expected: func() *StmtExecutePacket {
				p := NewStmtExecutePacket(flags.ClientQueryAttributes, 1)
				p.status = 0x17
				p.statementID = 1
				p.flags = 0x00
				p.iterationCount = 1
				p.parameterCount = 1
				p.nullBitmap = []byte{0x00}
				p.newParamsBindFlag = 0x01
				p.parameters = []StmtExecuteParameter{
					{
						Type:  packet.MySQLTypeString,
						Name:  "foo",
						Value: "bar",
					},
				}
				return p
			}(),
			errAssertFunc: assert.NoError,
		},
		{
			name: "多个参数",
			payload: []byte{
				// status
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
				// params[0].Type 第一个字节表示:FIELD_TYPE_LONGLONG (8), 第二个字节表示无符号 Unsigned: 0
				0x08, 0x00,
				// params[1].Type
				0x08, 0x00,
				// params[0].Value
				0x15, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
				// params[1].Value
				0x16, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
			},
			numParams: 2,
			expected: func() *StmtExecutePacket {
				p := NewStmtExecutePacket(0, 2)
				p.status = 0x17
				p.statementID = 1
				p.iterationCount = 1
				p.parameterCount = uint64(2)
				p.nullBitmap = []byte{0x00}
				p.newParamsBindFlag = 0x01
				p.parameters = []StmtExecuteParameter{
					{
						Type:  0x08,
						Value: int64(21),
					},
					{
						Type:  0x08,
						Value: int64(22),
					},
				}
				return p
			}(),
			errAssertFunc: assert.NoError,
		},
		{
			name:          "命令非法",
			payload:       []byte{0x18, 0x01, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01}, // Command is not 0x17
			numParams:     1,
			expected:      &StmtExecutePacket{},
			errAssertFunc: assert.Error,
		},
		{
			name:          "格式错误",
			payload:       []byte{0x17, 0x01, 0x00},
			numParams:     1,
			expected:      &StmtExecutePacket{},
			errAssertFunc: assert.Error,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := NewStmtExecutePacket(tt.clientCapabilityFlags, tt.numParams)
			err := req.Parse(tt.payload)
			tt.errAssertFunc(t, err)
			if err == nil {
				assert.Equal(t, tt.expected, req)
			}
		})
	}
}

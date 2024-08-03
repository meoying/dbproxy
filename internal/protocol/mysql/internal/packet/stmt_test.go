package packet

import (
	"testing"

	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/flags"
	"github.com/stretchr/testify/assert"
)

func TestPrepareStmtRequestParser_Parse(t *testing.T) {
	tests := []struct {
		name    string
		payload []byte

		wantCommand byte
		wantQuery   string
		wantErr     assert.ErrorAssertionFunc
	}{
		{
			name: "正常情况",
			payload: []byte{
				0x16, 0x53, 0x45, 0x4c, 0x45, 0x43, 0x54, 0x20, 0x43, 0x4f, 0x4e, 0x43, 0x41, 0x54,
				0x28, 0x3f, 0x2c, 0x20, 0x3f, 0x29, 0x20, 0x41, 0x53, 0x20, 0x63, 0x6f, 0x6c, 0x31,
			},
			wantCommand: 0x16,
			wantQuery:   "SELECT CONCAT(?, ?) AS col1",
			wantErr:     assert.NoError,
		},
		{
			name:    "载荷长度为0",
			payload: []byte{},
			wantErr: assert.Error,
		},
		{
			name: "命令字段错误",
			payload: []byte{
				0x17, 0x53, 0x45, 0x4c, 0x45, 0x43, 0x54, 0x20, 0x43, 0x4f, 0x4e, 0x43, 0x41, 0x54,
				0x28, 0x3f, 0x2c, 0x20, 0x3f, 0x29, 0x20, 0x41, 0x53, 0x20, 0x63, 0x6f, 0x6c, 0x31,
			},
			wantErr: assert.Error,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := NewPrepareStmtRequestParser()
			err := p.Parse(tt.payload)
			tt.wantErr(t, err)
			if err != nil {
				return
			}
			assert.Equal(t, tt.wantCommand, p.Command())
			assert.Equal(t, tt.wantQuery, p.Query())
		})
	}
}

func TestPrepareStmtResponseBuilder_BuildOK(t *testing.T) {
	t.Skip()
	tests := []struct {
		name       string
		getBuilder func(t *testing.T) *PrepareStmtResponseBuilder
		wantResp   []byte
	}{
		// {
		// 	name: "for a prepared query like SELECT CONCAT(?, ?) AS col1 and no CLIENT_OPTIONAL_RESULTSET_METADATA",
		// 	getBuilder: func(t *testing.T) *PrepareStmtResponseBuilder {
		// 		b := NewPrepareStmtResponseBuilder(0)
		// 		b.StatementID = 1
		// 		b.NumColumns = 2
		// 		b.NumParams = 2
		// 		return b
		// 	},
		// 	wantResp: []byte{
		// 		0x0c, 0x00, 0x00, 0x01, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00,
		// 		0x17, 0x00, 0x00, 0x02, 0x03, 0x64, 0x65, 0x66, 0x00, 0x00, 0x00, 0x01, 0x3f, 0x00, 0x0c, 0x3f,
		// 		0x00, 0x00, 0x00, 0x00, 0x00, 0xfd, 0x80, 0x00, 0x00, 0x00, 0x00, 0x17, 0x00, 0x00, 0x03, 0x03,
		// 		0x64, 0x65, 0x66, 0x00, 0x00, 0x00, 0x01, 0x3f, 0x00, 0x0c, 0x3f, 0x00, 0x00, 0x00, 0x00, 0x00,
		// 		0xfd, 0x80, 0x00, 0x00, 0x00, 0x00, 0x05, 0x00, 0x00, 0x04, 0xfe, 0x00, 0x00, 0x02, 0x00, 0x1a,
		// 		0x00, 0x00, 0x05, 0x03, 0x64, 0x65, 0x66, 0x00, 0x00, 0x00, 0x04, 0x63, 0x6f, 0x6c, 0x31, 0x00,
		// 		0x0c, 0x3f, 0x00, 0x00, 0x00, 0x00, 0x00, 0xfd, 0x80, 0x00, 0x1f, 0x00, 0x00, 0x05, 0x00, 0x00,
		// 		0x06, 0xfe, 0x00, 0x00, 0x02, 0x00,
		// 	},
		// },
		{
			name: "Prepare语句'DO 1'没有参数_没有结果集_未设置_CLIENT_OPTIONAL_RESULTSET_METADATA",
			getBuilder: func(t *testing.T) *PrepareStmtResponseBuilder {
				return &PrepareStmtResponseBuilder{FieldStatementID: 1}
			},
			wantResp: []byte{
				// packet header 下方比较时会被忽略
				0x0c, 0x00, 0x00, 0x01,
				// 载荷 payload
				0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := tt.getBuilder(t)
			assert.Equal(t, tt.wantResp[4:], b.BuildOK()[4:])
		})
	}
}

func TestExecuteStmtRequestParser_Parse(t *testing.T) {
	tests := []struct {
		name                  string
		payload               []byte
		numParams             uint64
		clientCapabilityFlags flags.CapabilityFlags
		expected              *ExecuteStmtRequestParser
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
			expected: func() *ExecuteStmtRequestParser {
				p := NewExecuteStmtRequestParser(0, 0)
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
			expected: func() *ExecuteStmtRequestParser {
				p := NewExecuteStmtRequestParser(0, 1)
				p.status = 0x17
				p.statementID = 1
				p.iterationCount = 1
				p.nullBitmap = []byte{0x00}
				p.newParamsBindFlag = 0x01
				p.parameterCount = 1
				p.parameters = []ExecuteStmtRequestParameter{
					{
						Type:  MySQLTypeLongLong,
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
			expected: func() *ExecuteStmtRequestParser {
				p := NewExecuteStmtRequestParser(0, 1)
				p.status = 0x17
				p.statementID = 1
				p.iterationCount = 1
				p.nullBitmap = []byte{0x00}
				p.newParamsBindFlag = 0x01
				p.parameterCount = 1
				p.parameters = []ExecuteStmtRequestParameter{
					{
						Type:  MySQLTypeLongLong,
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
			expected: func() *ExecuteStmtRequestParser {
				p := NewExecuteStmtRequestParser(0, 1)
				p.status = 0x17
				p.statementID = 1
				p.iterationCount = 1
				p.parameterCount = 1
				p.nullBitmap = []byte{0x00}
				p.newParamsBindFlag = 0x01
				p.parameters = []ExecuteStmtRequestParameter{
					{Type: MySQLTypeVarchar, Value: "foo"},
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
				b = append(b, LengthEncodeString("foo")...)
				// parameter_value
				b = append(b, LengthEncodeString("bar")...)
				return b
			}(),
			numParams:             1,
			clientCapabilityFlags: flags.ClientQueryAttributes,
			expected: func() *ExecuteStmtRequestParser {
				p := NewExecuteStmtRequestParser(flags.ClientQueryAttributes, 1)
				p.status = 0x17
				p.statementID = 1
				p.flags = 0x00
				p.iterationCount = 1
				p.parameterCount = 1
				p.nullBitmap = []byte{0x00}
				p.newParamsBindFlag = 0x01
				p.parameters = []ExecuteStmtRequestParameter{
					{
						Type:  MySQLTypeString,
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
				// params[0].Type 第一个字节表示:FIELD_TYPE_LONGLONG (8), 第二个字节表示 unsigned Unsigned: 0
				0x08, 0x00,
				// params[1].Type
				0x08, 0x00,
				// params[0].Value
				0x15, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
				// params[1].Value
				0x16, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
			},
			numParams: 2,
			expected: func() *ExecuteStmtRequestParser {
				p := NewExecuteStmtRequestParser(0, 2)
				p.status = 0x17
				p.statementID = 1
				p.iterationCount = 1
				p.parameterCount = uint64(2)
				p.nullBitmap = []byte{0x00}
				p.newParamsBindFlag = 0x01
				p.parameters = []ExecuteStmtRequestParameter{
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
			expected:      &ExecuteStmtRequestParser{},
			errAssertFunc: assert.Error,
		},
		{
			name:          "格式错误",
			payload:       []byte{0x17, 0x01, 0x00},
			numParams:     1,
			expected:      &ExecuteStmtRequestParser{},
			errAssertFunc: assert.Error,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := NewExecuteStmtRequestParser(tt.clientCapabilityFlags, tt.numParams)
			err := req.Parse(tt.payload)
			tt.errAssertFunc(t, err)
			if err == nil {
				assert.Equal(t, tt.expected, req)
			}
		})
	}
}

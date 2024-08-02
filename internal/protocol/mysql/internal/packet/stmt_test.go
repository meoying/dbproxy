package packet

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"strings"
	"testing"

	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/flags"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
				b := NewPrepareStmtResponseBuilder(0)
				b.StatementID = 1
				return b
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

func TestExecuteStmtRequest_Parse(t *testing.T) {
	t.Skip()
	tests := []struct {
		name          string
		payload       []byte
		numParams     uint64
		expected      ExecuteStmtRequestParser
		expectErrFunc require.ErrorAssertionFunc
	}{
		{
			name:          "payload命令字段非法",
			payload:       []byte{0x16, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00},
			expected:      ExecuteStmtRequestParser{},
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
			expected: ExecuteStmtRequestParser{
				statementID:    1,
				flags:          0,
				iterationCount: 1,
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
			expected: ExecuteStmtRequestParser{
				statementID:       1,
				flags:             0,
				iterationCount:    1,
				parameterCount:    1,
				nullBitmap:        []byte{0x00},
				newParamsBindFlag: 0x01,
				parameters: []ExecuteStmtRequestParameter{
					{Type: 15, Name: "foo"},
				},
			},
			expectErrFunc: require.NoError,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var req ExecuteStmtRequestParser
			tt.expectErrFunc(t, req.Parse(tt.payload))
			assert.Equal(t, tt.expected, req)
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
		err                   error
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
		// 	expected: ExecuteStmtRequestParser{
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
			name: "无参数",
			payload: func() []byte {
				var buf bytes.Buffer
				_ = binary.Write(&buf, binary.LittleEndian, byte(0x17)) // command
				_ = binary.Write(&buf, binary.LittleEndian, uint32(1))  // statement_id
				_ = binary.Write(&buf, binary.LittleEndian, byte(0x00)) // flags
				_ = binary.Write(&buf, binary.LittleEndian, uint32(1))  // iteration_count

				return buf.Bytes()
			}(),
			numParams: 0,
			expected: func() *ExecuteStmtRequestParser {
				b := NewExecuteStmtRequestParser(0, 0)
				b.status = 0x17
				b.statementID = 1
				b.iterationCount = 1
				return b
			}(),
			err: nil,
		},

		{
			name: "单个参数",
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
			expected: func() *ExecuteStmtRequestParser {
				b := NewExecuteStmtRequestParser(0, 1)
				b.status = 0x17
				b.statementID = 1
				b.iterationCount = 1
				b.nullBitmap = []byte{0x00}
				b.newParamsBindFlag = 0x01
				b.parameterCount = 1
				b.parameters = []ExecuteStmtRequestParameter{
					{
						Type:  MySQLTypeLongLong,
						Value: uint64(1002),
					},
				}
				return b
			}(),
			err: nil,
		},
		{
			name: "单个参数_负数",
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
				// Value (INT64): -128
				0x80, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
			},
			numParams: 1,
			expected: func() *ExecuteStmtRequestParser {
				b := NewExecuteStmtRequestParser(0, 1)
				b.status = 0x17
				b.statementID = 1
				b.iterationCount = 1
				b.nullBitmap = []byte{0x00}
				b.newParamsBindFlag = 0x01
				b.parameterCount = 1
				b.parameters = []ExecuteStmtRequestParameter{
					{
						Type:  MySQLTypeLongLong,
						Value: int64(-128),
					},
				}
				return b
			}(),
			err: nil,
		},
		// {
		// 	name: "单个参数_字符串类型",
		// 	payload: []byte{
		// 		0x17,                   // status
		// 		0x01, 0x00, 0x00, 0x00, // statement_id
		// 		0x00,                   // flags
		// 		0x01, 0x00, 0x00, 0x00, // iteration_count
		// 		0x01,       // parameter_count
		// 		0x00,       // null_bitmap
		// 		0x01,       // new_params_bind_flag
		// 		0x0f, 0x00, // parameter_type
		// 		0x03, 0x66, 0x6f, 0x6f, // parameter_name "foo"
		// 	},
		// 	numParams:             uint64(1),
		// 	clientCapabilityFlags: flags.ClientQueryAttributes,
		// 	expected: func() *ExecuteStmtRequestParser {
		//
		// 		b := NewExecuteStmtRequestParser(flags.ClientQueryAttributes, 1)
		// 		b.status = 0x17
		// 		b.statementID = 1
		// 		b.iterationCount = 1
		// 		b.parameterCount = 1
		// 		b.nullBitmap = []byte{0x00}
		// 		b.newParamsBindFlag = 0x01
		// 		b.parameters = []ExecuteStmtRequestParameter{
		// 			{Type: MySQLTypeVarchar, Name: "foo"},
		// 		}
		// 		return b
		// 	}(),
		// 	// expectErrFunc: require.NoError,
		// },
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
				b := NewExecuteStmtRequestParser(0, 2)
				b.status = 0x17
				b.statementID = 1
				b.iterationCount = 1
				b.parameterCount = uint64(2)
				b.nullBitmap = []byte{0x00}
				b.newParamsBindFlag = 0x01
				b.parameters = []ExecuteStmtRequestParameter{
					{
						Type:  0x08,
						Value: uint64(21),
					},
					{
						Type:  0x08,
						Value: uint64(22),
					},
				}
				return b
			}(),
			err: nil,
		},
		{
			name:      "命令非法",
			payload:   []byte{0x18, 0x01, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01}, // Command is not 0x17
			numParams: 1,
			expected:  &ExecuteStmtRequestParser{},
			err:       fmt.Errorf("invalid Command: 18"),
		},
		{
			name:      "格式错误",
			payload:   []byte{0x17, 0x01, 0x00},
			numParams: 1,
			expected:  &ExecuteStmtRequestParser{},
			err:       fmt.Errorf("error reading StatementID: unexpected EOF"),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := NewExecuteStmtRequestParser(tt.clientCapabilityFlags, tt.numParams)
			err := req.Parse(tt.payload)
			if tt.err != nil {
				assert.EqualError(t, err, tt.err.Error())
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, req)
			}
		})
	}
}

func getStringTypeQuery(columns ...string) string {

	tmpl := "SELECT /*useMaster*/ `id`,`type_char`, `type_varchar`, `type_tinytext`, `type_text`, `type_mediumtext`, `type_longtext`, `type_enum`, `type_set`, `type_binary`, `type_varbinary`, `type_json`, `type_bit` FROM `test_string_type` WHERE %s"

	for i := range columns {
		columns[i] = fmt.Sprintf("`%s` = ?", strings.Trim(columns[i], "`"))
	}

	return fmt.Sprintf(tmpl, strings.Join(columns, " AND "))
}

func TestQ(t *testing.T) {
	t.Log(getStringTypeQuery("id"))
	t.Log(getStringTypeQuery("`id`"))
	t.Log(getStringTypeQuery(`id`, `type_char`, `type_varchar`, `type_tinytext`, `type_text`, `type_mediumtext`, `type_longtext`, `type_enum`, `type_set`, `type_binary`, `type_varbinary`, `type_json`, `type_bit`))

}

package parser_test

import (
	"testing"

	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/packet/parser"
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
			p := parser.NewPrepareStmtRequestParser()
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

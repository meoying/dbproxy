package builder_test

import (
	"testing"

	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/flags"
	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/packet"
	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/packet/builder"
	"github.com/stretchr/testify/assert"
)

func TestStmtPrepareOKPacket_Build(t *testing.T) {
	tests := []struct {
		name       string
		getBuilder func(t *testing.T) *builder.StmtPrepareOKPacket
		wantResp   []byte
	}{
		{
			name: "Prepare语句'DO 1'没有参数_没有结果集_未设置_CLIENT_OPTIONAL_RESULTSET_METADATA",
			getBuilder: func(t *testing.T) *builder.StmtPrepareOKPacket {
				b := builder.NewStmtPrepareOKPacket(0, flags.ServerStatusAutoCommit, packet.CharSetUtf8mb4GeneralCi)
				b.StatementID = 1
				return b
			},
			wantResp: []byte{
				0x0c, 0x00, 0x00, 0x01, // packet header 下方比较时会被忽略
				0x00,                   // status
				0x01, 0x00, 0x00, 0x00, // statement_id
				0x00, 0x00, // num_columns
				0x00, 0x00, // num_params
				0x00,       // reserved
				0x00, 0x00, // warning_count
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := tt.getBuilder(t)
			var p []byte
			for _, pkt := range b.Build() {
				p = append(p, pkt...)
			}
			assert.Equal(t, tt.wantResp[4:], p[4:])
		})
	}
}

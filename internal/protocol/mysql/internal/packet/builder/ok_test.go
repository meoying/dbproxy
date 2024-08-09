package builder

import (
	"testing"

	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/flags"
	"github.com/stretchr/testify/assert"
)

func TestOKPacket_Build(t *testing.T) {
	tests := []struct {
		name string
		b    *OKPacket
		want []byte
	}{
		{
			name: "OK",
			b: func() *OKPacket {
				return NewOKPacket(flags.CapabilityFlags(flags.ClientProtocol41), flags.ServerStatusAutoCommit)
			}(),
			want: []byte{
				0x07, 0x00, 0x00, 0x02, // packet header
				0x00,       // OK header
				0x00,       // affected_rows
				0x00,       // last_insert_id
				0x02, 0x00, // status_flags
				0x00, 0x00, // warnings
			},
		},
		{
			name: "EOF",
			b: func() *OKPacket {
				return NewEOFProtocol41Packet(flags.CapabilityFlags(flags.ClientProtocol41), flags.ServerStatusAutoCommit)
			}(),
			want: []byte{
				0x07, 0x00, 0x00, 0x02, // packet header
				0xFE,       // EOF header
				0x00,       // affected_rows
				0x00,       // last_insert_id
				0x02, 0x00, // status_flags
				0x00, 0x00, // warnings
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want[4:], tt.b.Build()[4:])
		})
	}
}

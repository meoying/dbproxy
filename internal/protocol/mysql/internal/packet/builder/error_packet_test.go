package builder

import (
	"errors"
	"testing"

	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/flags"
	"github.com/stretchr/testify/assert"
)

func TestErrorPacketBuilder_Build(t *testing.T) {
	tests := []struct {
		name                  string
		ClientCapabilityFlags flags.CapabilityFlags
		Error                 Error
		want                  []byte
	}{
		{
			name:                  "客户端携带ClientProtocol41",
			ClientCapabilityFlags: flags.CapabilityFlags(flags.ClientProtocol41),
			Error:                 NewInternalError(errors.New("no tables used")),
			want: []byte{
				0xff,       // header
				0x76, 0x05, // error_code
				0x23,                         // sql_state_marker
				0x48, 0x59, 0x30, 0x30, 0x30, // sql_state
				0x49, 0x6e, 0x74, 0x65, 0x72, 0x6e, 0x61, 0x6c, 0x20, 0x65, 0x72, 0x72, 0x6f, // error_message
				0x72, 0x3a, 0x20, 0x6e, 0x6f, 0x20, 0x74, 0x61, 0x62, 0x6c, 0x65, 0x73, 0x20, 0x75, 0x73, 0x65, 0x64,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := ErrorPacketBuilder{
				ClientCapabilityFlags: tt.ClientCapabilityFlags,
				Error:                 tt.Error,
			}
			assert.Equal(t, tt.want, b.Build()[4:])
		})
	}
}

package builder_test

import (
	"testing"

	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/packet/builder"
	"github.com/stretchr/testify/assert"
)

func TestSetHeader_Build(t *testing.T) {
	tests := []struct {
		name          string
		builder       *builder.SetHeader
		want          []byte
		assertErrFunc assert.ErrorAssertionFunc
	}{
		{
			name: "正常情况",
			builder: func() *builder.SetHeader {
				return builder.NewSetHeader(1, make([]byte, 8))
			}(),
			want: []byte{
				0x04, 0x00, 0x00, // packet payload length
				0x01, // sequence
				0x00, 0x00, 0x00, 0x00,
			},
			assertErrFunc: assert.NoError,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := tt.builder.Build()
			tt.assertErrFunc(t, err)
			if err != nil {
				return
			}
			assert.Equal(t, tt.want, got)
		})
	}
}

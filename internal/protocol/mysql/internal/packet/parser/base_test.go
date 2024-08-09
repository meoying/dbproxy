package parser

import (
	"bytes"
	"testing"

	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/packet/encoding"
	"github.com/stretchr/testify/assert"
)

func TestBase_ParseLengthEncodedInteger(t *testing.T) {
	tests := []struct {
		name string
		buf  *bytes.Buffer

		wantInteger   uint64
		wantByteSize  int
		assertErrFunc assert.ErrorAssertionFunc
	}{
		{
			name: "n=0",
			buf: func() *bytes.Buffer {
				return bytes.NewBuffer(encoding.LengthEncodeInteger(0))
			}(),
			wantInteger:   uint64(0),
			wantByteSize:  1,
			assertErrFunc: assert.NoError,
		},
		{
			name: "0<n<251",
			buf: func() *bytes.Buffer {
				return bytes.NewBuffer(encoding.LengthEncodeInteger(250))
			}(),
			wantInteger:   uint64(250),
			wantByteSize:  1,
			assertErrFunc: assert.NoError,
		},
		{
			name: "n=251",
			buf: func() *bytes.Buffer {
				return bytes.NewBuffer(encoding.LengthEncodeInteger(251))
			}(),
			wantInteger:   uint64(251),
			wantByteSize:  2,
			assertErrFunc: assert.NoError,
		},
		{
			name: "251<n<2<<16",
			buf: func() *bytes.Buffer {
				return bytes.NewBuffer(encoding.LengthEncodeInteger(1<<16 - 1))
			}(),
			wantInteger:   uint64(1<<16 - 1),
			wantByteSize:  2,
			assertErrFunc: assert.NoError,
		},
		{
			name: "n=1<<16",
			buf: func() *bytes.Buffer {
				return bytes.NewBuffer(encoding.LengthEncodeInteger(1 << 16))
			}(),
			wantInteger:   uint64(1 << 16),
			wantByteSize:  3,
			assertErrFunc: assert.NoError,
		},
		{
			name: "2<<16<n<2<<24",
			buf: func() *bytes.Buffer {
				return bytes.NewBuffer(encoding.LengthEncodeInteger(1<<24 - 1))
			}(),
			wantInteger:   uint64(1<<24 - 1),
			wantByteSize:  3,
			assertErrFunc: assert.NoError,
		},
		{
			name: "n=2<<24",
			buf: func() *bytes.Buffer {
				return bytes.NewBuffer(encoding.LengthEncodeInteger(1 << 24))
			}(),
			wantInteger:   uint64(1 << 24),
			wantByteSize:  8,
			assertErrFunc: assert.NoError,
		},
		{
			name: "n=2<<24<n<2<<64",
			buf: func() *bytes.Buffer {
				return bytes.NewBuffer(encoding.LengthEncodeInteger(1<<64 - 1))
			}(),
			wantInteger:   uint64(1<<64 - 1),
			wantByteSize:  8,
			assertErrFunc: assert.NoError,
		},
		{
			name: "非法首字符",
			buf: func() *bytes.Buffer {
				return bytes.NewBuffer([]byte{0xff})
			}(),
			assertErrFunc: assert.Error,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			p := &base{}
			got, byteSize, err := p.ParseLengthEncodedInteger(tt.buf)
			tt.assertErrFunc(t, err)
			if err != nil {
				return
			}
			assert.Equal(t, tt.wantInteger, got)
			assert.Equal(t, tt.wantByteSize, byteSize)
		})
	}
}

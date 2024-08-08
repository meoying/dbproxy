package packet

import "bytes"

type Buffer struct {
	*bytes.Buffer
}

func NewBuffer() *Buffer {
	return &Buffer{new(bytes.Buffer)}
}

package composite

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestString(t *testing.T) {
	hash, err := NewHash(map[string]any{
		"key":  "user_id",
		"base": 10,
	})
	require.NoError(t, err)
	require.Equal(t, &Hash{Key: "user_id", Base: 10}, hash)
}

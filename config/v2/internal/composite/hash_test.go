package composite

import (
	"testing"

	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func TestHash(t *testing.T) {
	out, err := yaml.Marshal(map[string]any{
		"key":  "user_id",
		"base": 10,
	})
	require.NoError(t, err)

	h := &Hash{}
	err = yaml.Unmarshal(out, h)

	require.NoError(t, err)
	require.Equal(t, &Hash{Key: "user_id", Base: 10}, h)
}

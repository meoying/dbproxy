package composite

import (
	"fmt"
)

// Hash 类型
type Hash struct {
	Key  string `yaml:"key"`
	Base int    `yaml:"base"`
}

func (h *Hash) Type() string {
	return "hash"
}

func (h *Hash) IsZero() bool {
	return h.Key == "" && h.Base == 0
}

func (h *Hash) Evaluate() (map[string]string, error) {
	strs := make(map[string]string, h.Base)
	for i := 0; i < h.Base; i++ {
		key := fmt.Sprintf("%d", i)
		strs[key] = key
	}
	return strs, nil
}

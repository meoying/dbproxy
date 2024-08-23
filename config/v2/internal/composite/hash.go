package composite

import (
	"fmt"

	"gopkg.in/yaml.v3"
)

// Hash 类型
type Hash struct {
	Name string
	Key  string `yaml:"key"`
	Base int    `yaml:"base"`
}

func NewHash(values map[string]any) (*Hash, error) {
	out, err := yaml.Marshal(values)
	if err != nil {
		return nil, err
	}
	h := &Hash{}
	err = yaml.Unmarshal(out, h)
	return h, err
}

func (h *Hash) IsZeroValue() bool {
	return h.Key == "" && h.Base == 0
}

func (h *Hash) Evaluate() (map[string]string, error) {
	strs := make(map[string]string, h.Base)
	for i := 0; i < h.Base; i++ {
		key := fmt.Sprintf("%d", i)
		strs[key] = key
	}
	return strs, nil

	// strs := make([]string, h.Base)
	// for i := 0; i < h.Base; i++ {
	// 	strs[i] = fmt.Sprintf("%d", i)
	// }
	// return strs, nil
}

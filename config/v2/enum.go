package v2

// Enum 枚举类型
type Enum []string

func (e Enum) Evaluate() (map[string]string, error) {
	mp := make(map[string]string, len(e))
	for _, v := range e {
		mp[v] = v
	}
	return mp, nil
}

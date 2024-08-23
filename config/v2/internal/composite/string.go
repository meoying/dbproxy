package composite

type String string

func (s String) Evaluate() (map[string]string, error) {
	return map[string]string{string(s): string(s)}, nil
}

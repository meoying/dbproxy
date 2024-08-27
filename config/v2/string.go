package v2

type String string

func (s String) IsZero() bool {
	return len(s) == 0
}

func (s String) Evaluate() (map[string]string, error) {
	return map[string]string{string(s): string(s)}, nil
}

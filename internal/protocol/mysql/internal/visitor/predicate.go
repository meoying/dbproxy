package visitor

// Predicate will be used in Where Or Having
type Predicate BinaryExpr

func (Predicate) expr() (string, error) {
	return "", nil
}

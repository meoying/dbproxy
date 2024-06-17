package visitor

// Predicate will be used in Where Or Having
type Predicate BinaryExpr

var emptyPredicate = Predicate{}

func (Predicate) expr() (string, error) {
	return "", nil
}

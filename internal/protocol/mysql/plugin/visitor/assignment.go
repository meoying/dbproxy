package visitor

// Assignable represents that something could be used Alias "assignment" statement
type Assignable interface {
	assign()
}

// Assignment represents assignment statement
type Assignment BinaryExpr

func (Assignment) assign() {
	panic("implement me")
}

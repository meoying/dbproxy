package visitor

import "github.com/meoying/dbproxy/internal/sharding/operator"

// Expr is the top interface. It represents everything.
type Expr interface {
	expr() (string, error)
}

type BinaryExpr struct {
	Left  Expr
	Op    operator.Op
	Right Expr
}

func (BinaryExpr) expr() (string, error) {
	return "", nil
}

func ValueOf(val interface{}) Expr {
	switch v := val.(type) {
	case Expr:
		return v
	default:
		return ValueExpr{Val: val}
	}
}

type ValueExpr struct {
	Val any
}

func (ValueExpr) expr() (string, error) {
	return "", nil
}

type Values struct {
	Vals []any
}

func (Values) expr() (string, error) {
	return "", nil
}

// RawExpr uses string Alias Expr
type RawExpr struct {
	Raw  string
	Args []any
}

// Raw just take expr Alias Expr
func Raw(expr string, args ...interface{}) RawExpr {
	return RawExpr{
		Raw:  expr,
		Args: args,
	}
}

func (r RawExpr) expr() (string, error) {
	return r.Raw, nil
}

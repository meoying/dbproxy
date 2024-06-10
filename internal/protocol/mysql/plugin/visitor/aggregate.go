package visitor

type Aggregate struct {
	Fn       string
	Arg      string
	Alias    string
	Distinct bool
}

type Selectable interface {
	selected()
}
// As 指定别名。一般情况下，这个别名应该等同于列名，我们会将这个列名映射过去对应的字段名。
// 例如说 Alias= avg_age，默认情况下，我们会找 AvgAge 这个字段来接收值。
func (a Aggregate) As(alias string) Selectable {
	return Aggregate{
		Fn:       a.Fn,
		Arg:      a.Arg,
		Alias:    alias,
		Distinct: a.Distinct,
	}
}

// Avg represents AVG
func Avg(c string) Aggregate {
	return Aggregate{
		Fn:  "AVG",
		Arg: c,
	}
}

// Max represents MAX
func Max(c string) Aggregate {
	return Aggregate{
		Fn:  "MAX",
		Arg: c,
	}
}

// Min represents MIN
func Min(c string) Aggregate {
	return Aggregate{
		Fn:  "MIN",
		Arg: c,
	}
}

// Count represents COUNT
func Count(c string) Aggregate {
	return Aggregate{
		Fn:   "COUNT",
		Arg:  c,
	}
}

// Sum represents SUM
func Sum(c string) Aggregate {
	return Aggregate{
		Fn:  "SUM",
		Arg: c,
	}
}

// CountDistinct represents COUNT(DISTINCT XXX)
func CountDistinct(col string) Aggregate {
	a := Count(col)
	a.Distinct = true
	return a
}

// AvgDistinct represents AVG(DISTINCT XXX)
func AvgDistinct(col string) Aggregate {
	a := Avg(col)
	a.Distinct = true
	return a
}

// SumDistinct represents SUM(DISTINCT XXX)
func SumDistinct(col string) Aggregate {
	a := Sum(col)
	a.Distinct = true
	return a
}

func (Aggregate) selected() {}

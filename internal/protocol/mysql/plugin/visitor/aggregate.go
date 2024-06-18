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

func (Aggregate) selected() {}

func NewAggregate(name string, fn string) Aggregate {
	return Aggregate{
		Arg: name,
		Fn:  fn,
	}
}
func NewDistinctAggregate(name string, fn string) Aggregate {
	return Aggregate{
		Arg:      name,
		Fn:       fn,
		Distinct: true,
	}
}

func (a Aggregate) As(alias string) Aggregate {
	return Aggregate{
		Arg:      a.Arg,
		Fn:       a.Fn,
		Alias:    alias,
		Distinct: a.Distinct,
	}
}

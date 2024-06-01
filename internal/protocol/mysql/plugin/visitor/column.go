package visitor

type Column struct {
	Name  string
	Alias string
}

func (Column) assign() {
	panic("implement me")
}

func (Column) expr() (string, error) {
	panic("implement me")
}



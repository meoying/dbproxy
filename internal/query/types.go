package query

type Query struct {
	SQL        string
	Args       []any
	DB         string
	Datasource string
}

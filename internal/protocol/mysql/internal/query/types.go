package query

type Query struct {
	SQL    string
	Params []Param
}

type Param struct {
	// 是否是 NULL
	IsNull bool
	// 参数类型
	Type       MySQLType
	IsUnsigned bool
	// 参数名字
	Name       string
	ValueBytes ParamValue
}

type ParamValue []byte

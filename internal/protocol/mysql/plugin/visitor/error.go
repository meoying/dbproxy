package visitor

import "github.com/pkg/errors"

var (
	errUnsupportedDeleteSql = errors.New("未支持的delete语句")
	errUnsupportedUpdateSql = errors.New("未支持的update语句")
	errInvalidSql           = errors.New("错误的sql的语句")
	errStmtMatch            = errors.New("当前语句不能使用该解析方式")
	errQueryInvalid         = errors.New("当前查询错误")
	errUnsupportedAggregate = errors.New("未支持的聚合函数")
	errUnsupportedOrderByClause = errors.New("未支持的OrderBy语句")
	errUnsupportedGroupByClause = errors.New("未支持的GroupBy语句")
)

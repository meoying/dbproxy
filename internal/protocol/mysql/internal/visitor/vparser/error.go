package vparser

import "github.com/pkg/errors"

var (
	errUnsupportedDeleteSql            = errors.New("未支持的delete语句")
	errUnsupportedUpdateSql            = errors.New("未支持的update语句")
	errStmtMatch                       = errors.New("当前语句不能使用该解析方式")
	errQueryInvalid                    = errors.New("当前查询错误")
	errUnsupportedOrderByClause        = errors.New("未支持的OrderBy语句")
	errUnsupportedGroupByClause        = errors.New("未支持的GroupBy语句")
	errUnsupportedMutilInsertByPrepare = errors.New("未支持的Prepare语句")
)

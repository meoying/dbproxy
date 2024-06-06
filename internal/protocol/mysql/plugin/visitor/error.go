package visitor

import "github.com/pkg/errors"

var (
	ErrUnsupportedDeleteSql = errors.New("未支持的delete语句")
	ErrUnsupportedUpdateSql  = errors.New("未支持的update语句")
	ErrInvalidSql = errors.New("错误的sql的语句")

)

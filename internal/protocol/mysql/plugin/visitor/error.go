package visitor

import "github.com/pkg/errors"

var (
	ErrUnsupportedDeleteSql = errors.New("未支持的delete语句")

)

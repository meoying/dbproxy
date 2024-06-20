package builder

import "github.com/pkg/errors"

var (
	errUnsupportedDeleteSql = errors.New("未支持的delete语句")
	errUnsupportedUpdateSql = errors.New("未支持的update语句")
)

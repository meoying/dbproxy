package builder

import "github.com/pkg/errors"

var (
	errUnsupportedUpdateSql = errors.New("未支持的update语句")
)

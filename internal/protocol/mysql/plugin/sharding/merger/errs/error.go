package errs

import "github.com/pkg/errors"

var (
	ErrMergerEmptyRows                   = errors.New("merger: sql.Rows列表为空")
	ErrMergerRowsClosed                  = errors.New("merger: Rows已经关闭")
	ErrMergerRowsDiff                    = errors.New("merger: sql.Rows列表中的字段不同")
	ErrMergerRowsIsNull                  = errors.New("merger: sql.Rows列表中有元素为nil")

)
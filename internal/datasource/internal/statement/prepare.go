package statement

import (
	"context"

	"github.com/meoying/dbproxy/internal/datasource"
)

func Prepare(ctx context.Context, f datasource.Finder) (*DelayStmt, error) {
	return NewDelayStmt(f), nil
}

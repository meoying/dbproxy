package statement

import (
	"context"
	"github.com/meoying/dbproxy/internal/datasource/internal/errs"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestNewStmtFacade_DelayStmtFactory(t *testing.T) {
	testCases := []struct {
		name    string
		ctx     context.Context
		wantRes StmtFactory
		wantErr error
	}{
		{
			name:    "new without delay",
			ctx:     UsingStmtType(context.Background(), ""),
			wantErr: errs.ErrUnsupportedDistributedPrepare,
		},
		{
			name:    "new with delay",
			ctx:     UsingStmtType(context.Background(), Delay),
			wantRes: DelayStmtFactory{},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			stmt, err := NewStmtFacade(tc.ctx, nil)
			if err != nil {
				assert.Equal(t, err, tc.wantErr)
			} else {
				assert.NotNil(t, stmt.factory)
				assert.Equal(t, DelayStmtFactory{}, stmt.factory)
			}
		})
	}
}

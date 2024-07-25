package sharding

import (
	"context"

	"github.com/meoying/dbproxy/internal/sharding"
)

type PrepareHandler struct {
}

func (p *PrepareHandler) Build(ctx context.Context) ([]sharding.Query, error) {
	// TODO implement me
	panic("implement me")
}

func (p *PrepareHandler) QueryOrExec(ctx context.Context) (*Result, error) {
	// TODO implement me
	panic("implement me")
}

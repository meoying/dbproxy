package sharding

import (
	"context"
	"database/sql"
	"github.com/meoying/dbproxy/internal/datasource"
	"github.com/meoying/dbproxy/internal/sharding"
	"go.uber.org/multierr"
	"sync"
)

//  几个dml语句共有的逻辑执行逻辑
func exec(ctx context.Context, db datasource.DataSource,qs []sharding.Query)sharding.Result  {
	errList := make([]error, len(qs))
	resList := make([]sql.Result, len(qs))
	var wg sync.WaitGroup
	locker := &sync.RWMutex{}
	wg.Add(len(qs))
	for idx, q := range qs {
		go func(idx int, q sharding.Query) {
			defer wg.Done()
			res, er := db.Exec(ctx, q)
			locker.Lock()
			errList[idx] = er
			resList[idx] = res
			locker.Unlock()
		}(idx, q)
	}
	wg.Wait()
	shardingRes := sharding.NewResult(resList, multierr.Combine(errList...))
	return shardingRes
}

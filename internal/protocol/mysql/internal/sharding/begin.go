package sharding

import (
	"context"
	"github.com/meoying/dbproxy/internal/datasource"
	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/pcontext"
	"github.com/meoying/dbproxy/internal/sharding"
)

type BeginHandler struct {
	algorithm sharding.Algorithm
	ds        datasource.DataSource
	ctx       *pcontext.Context

	// 这里要维持住很多的内容
	// 所有的事务放在这里，按照 map[tid] 来索引
	// txRepo 必须是全局唯一的单例，或者挪到 pcontext/Conn 里面？
	// txRepo TxRepository
}

func NewBeginHandler(a sharding.Algorithm, db datasource.DataSource, ctx *pcontext.Context) (ShardingHandler, error) {
	panic("implement me")
}

func (b *BeginHandler) Build(ctx context.Context) ([]sharding.Query, error) {
	panic("implement me")
}

func (b *BeginHandler) QueryOrExec(ctx context.Context) (*Result, error) {
	// 从 SQL 里面抽取出来 hint
	// 判定要执行哪一种事务，而后执行
	//switch txType {
	//case 'delay':
	// 在这里生成一个 tid
	// tid :=
	//tx := b.ds.BeginTx(NewDelayTxContext(context.Background()))
	// 在 txRepo 里面放好 tx
	//tx.Repo[tid] = tx
	// 把 tid 作为响应的一部分，写回去给客户端。客户端后续要带着这个 tid 来执行事务内部操作
	// tid 这个地方你可以写死（也就是你只支持事务逐个执行），等我找另外一个同学解决 tid 传递的问题
	//}
	// Rollback 和 commit 则是从 txrepo 里面拿到对应的事务，执行 Commit 或者 Rollback
	//TODO implement me
	panic("implement me")
}

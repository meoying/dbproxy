package rwsplit

import (
	"context"
	"strings"

	"github.com/meoying/dbproxy/internal/datasource"
	"github.com/meoying/dbproxy/internal/datasource/masterslave"
	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/ast/parser"
	pcontext "github.com/meoying/dbproxy/internal/protocol/mysql/internal/pcontext"
	"github.com/meoying/dbproxy/internal/protocol/mysql/plugin"
)

type Handler struct {
	// 使用接口是为了测试方便，但是在初始化函数里面
	// 传入的是 masterslave.Datasource
	ds datasource.DataSource
}

// Handle 读写分离的逻辑：是否 SELECT 语句以及是否包含强制使用主库的标记两者合并判断
// 1. SELECT 语句：默认走从库
// 2. 其它语句：默认走主库
// 3. 在 SELECT 语句中 proxy hint 中使用了 useMaster 的标记，则走主库
func (h *Handler) Handle(ctx *pcontext.Context) (*plugin.Result, error) {
	dml := ctx.ParsedQuery.FirstDML()
	sql := dml.GetChildren()[0]

	switch q := sql.(type) {
	case *parser.SelectStatementContext:
		// SELECT 语句
		// 进一步检测有没有proxy hint
		// 如果有的话，那么第一个肯定是 HINT，这是我们的语法规则定义的
		first := q.GetChildren()[0]
		hint, ok := first.(*parser.ProxyHintContext)
		useMaster := ok && strings.Contains(hint.GetText(), "useMaster")
		var newCtx context.Context = ctx
		if useMaster {
			newCtx = masterslave.UseMaster(newCtx)
		}
		res, err := h.ds.Query(newCtx, datasource.Query{
			SQL:  ctx.Query,
			Args: ctx.Args,
		})
		return &plugin.Result{
			Rows: res,
		}, err
	default:
		// 其余语句默认走 master
		res, err := h.ds.Exec(ctx, datasource.Query{
			SQL:  ctx.Query,
			Args: ctx.Args,
		})
		return &plugin.Result{
			Result: res,
		}, err
	}
}

func NewHandler(ds *masterslave.MasterSlavesDB) *Handler {
	return &Handler{
		ds: ds,
	}
}

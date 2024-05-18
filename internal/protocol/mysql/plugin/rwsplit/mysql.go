package rwsplit

import (
	"context"
	"github.com/antlr4-go/antlr/v4"
	"github.com/meoying/dbproxy/internal/plugin"
	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/ast/parser"
	"strings"
)

type MySQLHandler struct {
}

// Handle 读写分离的逻辑：是否 SELECT 语句以及是否包含强制使用主库的标记两者合并判断
// 1. SELECT 语句：默认走从库
// 2. 其它语句：默认走主库
// 3. 在注释中使用了 USE_MASTER 的标记，则走主库
func (h *MySQLHandler) Handle(ctx context.Context, query string, args ...any) (plugin.Result, error) {
	panic("implement me")
}

func (h *MySQLHandler) usingMaster(query string) bool {
	input := antlr.NewInputStream(query)
	lexer := parser.NewMySqlLexer(input)
	tokens := antlr.NewCommonTokenStream(lexer, antlr.TokenDefaultChannel)
	psr := parser.NewMySqlParser(tokens)
	v := &Visitor{parser: psr}
	psr.AddParseListener(v)
	// 试着解析为 SELECT 语句
	_ = psr.Root()
	// 这种判定方式只适合单一查询
	// 如果是 INSERT INTO SELECT xx 这种形态，会被误判
	isSelect := v.isSelectStatement()
	hints := v.proxyHints()
	// 确定注释里面有没有提示
	return strings.Contains(hints, "USE_MASTER") || !isSelect
}

var _ parser.MySqlParserListener = &Visitor{}

type Visitor struct {
	// 组合这个东西
	parser.BaseMySqlParserListener
	parser *parser.MySqlParser

	// 我们需要在遍历中找到的数据
	selectCtx *parser.SelectStatementContext
	// 我们自定义的语法提示
	hintsCtx *parser.ProxyHintContext
}

func (v *Visitor) EnterEveryRule(ctx antlr.ParserRuleContext) {
	if sctx, ok := ctx.(*parser.SelectStatementContext); ok {
		v.selectCtx = sctx
	}
	if hctx, ok := ctx.(*parser.ProxyHintContext); ok {
		v.hintsCtx = hctx
	}
}

func (v *Visitor) isSelectStatement() bool {
	return v.selectCtx != nil
}

func (v *Visitor) proxyHints() string {
	if v.hintsCtx != nil {
		return v.hintsCtx.GetText()
	}
	return ""
}

package builder

import (
	"fmt"
	"github.com/antlr4-go/antlr/v4"
	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/ast/parser"
	"strings"
)

type Base struct {
	db  string
	tab string
	*parser.BaseMySqlParserVisitor
}

// VisitTableName 用于篡改表名
func (b *Base) VisitTableName(ctx *parser.TableNameContext) any {
	// 设置库名，如果库名不存在就设置表名
	return b.VisitFullId(ctx.FullId().(*parser.FullIdContext))
}

func (b *Base) VisitFullId(ctx *parser.FullIdContext) any {
	// 去除db.tab的.tab部分
	for  len(ctx.GetChildren()) > 1 {
		ctx.RemoveLastChild()
	}
	// 获取db.tab的 db部分的token
	dbToken := ctx.Uid(0).GetStop()
	// 篡改值
	dbToken.SetText(b.withQuote(b.setDB()))
	if b.db == "" {
		// 说明只有tab，没有db
		return nil
	}
	// 新建一个db.tab tab部分的token
	token := antlr.NewCommonToken(dbToken.GetSource(), parser.MySqlParserDOT_ID, dbToken.GetChannel(), dbToken.GetStart(), dbToken.GetStop())
	// 设置token部分的值
	token.SetText(fmt.Sprintf(".%s", b.withQuote(b.tab)))
	// 将token加入ast树
	ctx.AddTokenNode(token)
	return nil
}

func (b *Base) withQuote(v string) string {
	return fmt.Sprintf("`%s`", v)
}

// 去除最后的eof标志
func (b *Base) removeEof(v string) string {
	return strings.TrimSuffix(v, "<EOF>")
}

func (b *Base) setDB() string {
	// 如果库名没有就返回表名
	if b.db == "" {
		return b.tab
	}
	return b.db
}

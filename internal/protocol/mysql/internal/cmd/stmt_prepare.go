package cmd

import (
	"context"
	"fmt"
	"strings"

	"github.com/meoying/dbproxy/internal/datasource/column"
	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/ast"
	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/connection"
	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/packet"
	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/pcontext"
	"github.com/meoying/dbproxy/internal/protocol/mysql/plugin"
)

var _ Executor = &StmtPrepareExecutor{}

type StmtPrepareExecutor struct {
	hdl    plugin.Handler
	stmtId int
}

func NewStmtPrepareExecutor(hdl plugin.Handler) *StmtPrepareExecutor {
	return &StmtPrepareExecutor{
		hdl: hdl,
	}
}

func (exec *StmtPrepareExecutor) Exec(
	ctx context.Context,
	conn *connection.Conn,
	payload []byte) error {
	originQue, parseQue := exec.parseQuery(payload)
	pctx := &pcontext.Context{
		Context: ctx,
		Query:   originQue,
		ParsedQuery: pcontext.ParsedQuery{
			Root: ast.Parse(parseQue),
		},
		StmtId: exec.stmtId,
	}

	// 在这里执行 que，并且写回响应
	result, err := exec.hdl.Handle(pctx)
	if err != nil {
		// 回写错误响应
		// 先返回系统错误
		errResp := packet.BuildErInternalError(err.Error())
		return conn.WritePacket(packet.BuildErrRespPacket(errResp))
	}

	// TODO 这里的字段可能要获取Prepare中展示的字段信息，不过也可以试试能不能瞎编数据
	fakeColumn := []column.Column{
		column.NewColumn("id", "INT"),
		column.NewColumn("name", "STRING"),
	}
	resp, err := exec.resp(result.StmtId, fakeColumn, exec.buildParamColumn(originQue), conn.CharacterSet())
	if err != nil {
		errResp := packet.BuildErInternalError(err.Error())
		return conn.WritePacket(packet.BuildErrRespPacket(errResp))
	}
	for _, pkt := range resp {
		err = conn.WritePacket(pkt)
		if err != nil {
			errResp := packet.BuildErInternalError(err.Error())
			return conn.WritePacket(packet.BuildErrRespPacket(errResp))
		}
	}
	// 记录prepare
	conn.Prepares[exec.stmtId] = true

	return nil
}

// parseQuery 获取sql语句
func (exec *StmtPrepareExecutor) parseQuery(payload []byte) (string, string) {
	// 第一个字节是 cmd
	originQue := string(payload[1:])
	exec.stmtId++
	parseQue := fmt.Sprintf("PREPARE stmt%d FROM '%s'", exec.stmtId, originQue)
	return originQue, parseQue
}

// resp 根据执行结果返回转换成对应的格式并返回
// response 的 COM_STMT_PREPARE
// https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_stmt_prepare.html
func (exec *StmtPrepareExecutor) resp(stmtId int, cols []column.Column, params []column.Column, charset uint32) ([][]byte, error) {
	var packetArr [][]byte

	//packetArr1 := [][]byte{
	//	{0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x02, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00},
	//	{0x00, 0x00, 0x00, 0x00, 0x03, 0x64, 0x65, 0x66, 0x00, 0x00, 0x00, 0x01, 0x3f, 0x00, 0x0c, 0x3f, 0x00, 0x15, 0x00, 0x00, 0x00, 0x08, 0x80, 0x00, 0x00, 0x00, 0x00},
	//	{0x00, 0x00, 0x00, 0x00, 0xfe, 0x00, 0x00, 0x02, 0x00},
	//	{0x00, 0x00, 0x00, 0x00, 0x03, 0x64, 0x65, 0x66, 0x04, 0x74, 0x65, 0x73, 0x74, 0x05, 0x75, 0x73, 0x65, 0x72, 0x73, 0x05, 0x75, 0x73, 0x65, 0x72, 0x73, 0x02, 0x69, 0x64, 0x02, 0x69, 0x64, 0x0c, 0x3f, 0x00, 0x0b, 0x00, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00, 0x00, 0x00},
	//	{0x00, 0x00, 0x00, 0x00, 0x03, 0x64, 0x65, 0x66, 0x04, 0x74, 0x65, 0x73, 0x74, 0x05, 0x75, 0x73, 0x65, 0x72, 0x73, 0x05, 0x75, 0x73, 0x65, 0x72, 0x73, 0x04, 0x6e, 0x61, 0x6d, 0x65, 0x04, 0x6e, 0x61, 0x6d, 0x65, 0x0c, 0x2d, 0x00, 0xc8, 0x00, 0x00, 0x00, 0xfd, 0x00, 0x00, 0x00, 0x00, 0x00},
	//	{0x00, 0x00, 0x00, 0x00, 0xfe, 0x00, 0x00, 0x02, 0x00},
	//}

	// 写入预处理信息包
	packetArr = append(packetArr, packet.BuildStmtPacket(stmtId, len(cols), len(params)))
	// 写入参数描述包
	for _, p := range params {
		packetArr = append(packetArr, packet.BuildColumnDefinitionPacket(p, charset))
	}

	packetArr = append(packetArr, packet.BuildEOFPacket())

	// 写入字段描述包
	for _, c := range cols {
		packetArr = append(packetArr, packet.BuildColumnDefinitionPacket(c, charset))
	}

	packetArr = append(packetArr, packet.BuildEOFPacket())

	return packetArr, nil
}

func (exec *StmtPrepareExecutor) countParamPlaceholders(sql string) int {
	return strings.Count(sql, "?")
}

// buildParamColumn 构建占位符字段数据
func (exec *StmtPrepareExecutor) buildParamColumn(sql string) []column.Column {
	count := exec.countParamPlaceholders(sql)

	col := make([]column.Column, 0, count)
	for i := 0; i < count; i++ {
		col = append(col, column.NewColumn("?", "BIGINT"))
	}
	return col
}

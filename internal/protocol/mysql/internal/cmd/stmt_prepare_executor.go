package cmd

import (
	"context"
	"fmt"
	"log"

	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/connection"
	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/packet"
	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/pcontext"
	"github.com/meoying/dbproxy/internal/protocol/mysql/plugin"
)

var (
	_ Executor          = &StmtPrepareExecutor{}
	_ packet.ColumnType = Column{}
)

// Column 直接传入数据，伪装成了一个 [packet.ColumnType] 非线程安全实现
type Column struct {
	name         string
	databaseType string
}

func NewColumn(name string, databaseType string) Column {
	return Column{
		name:         name,
		databaseType: databaseType,
	}
}

func (c Column) Name() string {
	return c.name
}

func (c Column) DatabaseTypeName() string {
	return c.databaseType
}

type StmtPrepareExecutor struct {
	hdl plugin.Handler
	*BaseStmtExecutor
}

func NewStmtPrepareExecutor(hdl plugin.Handler, executor *BaseStmtExecutor) *StmtPrepareExecutor {
	return &StmtPrepareExecutor{
		hdl:              hdl,
		BaseStmtExecutor: executor,
	}
}

/*
client --> prepare stmt FROM 'query' --> server 生成id, 构建query= PREPARE stmt{ID} FROM 'Query' -->

                                     <--  根据结果, stmtID=1,


*/

func (e *StmtPrepareExecutor) Exec(
	ctx context.Context,
	conn *connection.Conn,
	payload []byte) error {

	query := e.parseQuery(payload)
	stmtID := e.generateStmtID()
	numParams := e.storeNumParams(stmtID, query)

	prepareStmtSQL := e.generatePrepareStmtSQL(stmtID, query)

	log.Printf("Query = %s\n", query)
	log.Printf("PrepareStmtSQL = %s\n", prepareStmtSQL)

	pctx := &pcontext.Context{
		Context:     ctx,
		Query:       query,
		ParsedQuery: pcontext.NewParsedQuery(prepareStmtSQL, nil),
		ConnID:      conn.ID(),
		StmtID:      stmtID,
	}

	// 在这里执行 que，并且写回响应
	result, err := e.hdl.Handle(pctx)
	if err != nil {
		// 回写错误响应
		// 先返回系统错误
		return e.writeErrRespPacket(conn, err)
	}

	conn.SetInTransaction(result.InTransactionState)

	packets, err := e.buildStmtPrepareOKRespPacket(result.StmtID, numParams, conn.CharacterSet(), e.getServerStatus(conn))
	if err != nil {
		return e.writeErrRespPacket(conn, err)
	}

	return e.writeRespPackets(conn, packets)
}

// generatePrepareStmtSQL 获取创建prepare的sql语句
func (e *StmtPrepareExecutor) generatePrepareStmtSQL(stmtId uint32, query string) string {
	return fmt.Sprintf("PREPARE stmt%d FROM '%s'", stmtId, query)
}

// buildStmtPrepareOKRespPacket 根据执行结果返回转换成对应的格式并返回
// response 的 COM_STMT_PREPARE_OK
// https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_stmt_prepare.html#sect_protocol_com_stmt_prepare_response_ok
func (e *StmtPrepareExecutor) buildStmtPrepareOKRespPacket(stmtId uint32, numParams uint64, charset uint32, status packet.SeverStatus) ([][]byte, error) {
	var packets [][]byte

	params := e.buildParameterDefinitionBlock(numParams, charset)
	fields := e.buildColumnDefinitionBlock(numParams, charset)

	// 写入预处理信息包
	packets = append(packets, packet.BuildStmtPrepareRespPacket(int(stmtId), len(fields), len(params)))
	if len(params) > 0 {
		// 写入参数描述包
		packets = append(packets, params...)
		packets = append(packets, packet.BuildEOFPacket(status))
	}
	if len(fields) > 0 {
		// 写入字段描述包
		packets = append(packets, fields...)
		packets = append(packets, packet.BuildEOFPacket(status))
	}
	return packets, nil
}

// buildParameterDefinitionBlock 构建参数定义块
func (e *StmtPrepareExecutor) buildParameterDefinitionBlock(n uint64, charset uint32) [][]byte {
	params := make([]Column, 0, n)
	for i := uint64(0); i < n; i++ {
		params = append(params, NewColumn("?", "BIGINT"))
	}
	var packets [][]byte
	for _, p := range params {
		packets = append(packets, packet.BuildColumnDefinitionPacket(p, charset))
	}
	return packets
}

// buildColumnDefinitionBlock 构建列定义块
func (e *StmtPrepareExecutor) buildColumnDefinitionBlock(n uint64, charset uint32) [][]byte {
	// TODO 这里的字段可能要获取Prepare中展示的字段信息，不过也可以试试能不能瞎编数据
	// fields := []Column{
	// 	NewColumn("id", "INT"),
	// 	NewColumn("name", "STRING"),
	// }
	fields := make([]Column, 0, n+1)
	for i := uint64(0); i < n; i++ {
		fields = append(fields, NewColumn(fmt.Sprintf("fake_field_%d", i), "INT"))
	}
	var packets [][]byte
	for _, f := range fields {
		packets = append(packets, packet.BuildColumnDefinitionPacket(f, charset))
	}
	return packets
}

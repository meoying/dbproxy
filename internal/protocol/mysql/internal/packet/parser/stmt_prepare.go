package parser

import "fmt"

// StmtPreparePacket 用于解析客户端发送的 COM_STMT_PREPARE 包
// https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_stmt_prepare.html
type StmtPreparePacket struct {
	// int<1>	command	0x16: COM_STMT_PREPARE
	command byte
	// string<EOF>	query	The query to prepare
	query string
}

func NewStmtPreparePacket() *StmtPreparePacket {
	return &StmtPreparePacket{}
}

func (p *StmtPreparePacket) Parse(payload []byte) error {
	if len(payload) < 1 {
		return fmt.Errorf("请求格式非法: PrepareStmt")
	}
	if payload[0] != 0x16 {
		return fmt.Errorf("命令非法: %d", payload[0])
	}
	p.command = payload[0]
	p.query = string(payload[1:])
	return nil
}

func (p *StmtPreparePacket) Command() byte {
	return p.command
}

func (p *StmtPreparePacket) Query() string {
	return p.query
}

// Copyright 2021 ecodeclub
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cmd

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/ast"
	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/connection"
	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/packet"
	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/pcontext"
	"github.com/meoying/dbproxy/internal/protocol/mysql/plugin"
)

var _ Executor = &StmtCloseExecutor{}

type StmtCloseExecutor struct {
	hdl plugin.Handler
}

func NewStmtCloseExecutor(hdl plugin.Handler) *StmtCloseExecutor {
	return &StmtCloseExecutor{
		hdl: hdl,
	}
}

func (exec *StmtCloseExecutor) Exec(
	ctx context.Context,
	conn *connection.Conn,
	payload []byte) error {
	stmtId := exec.parseStmtId(payload)
	parseQue := exec.parseQuery(stmtId)
	pctx := &pcontext.Context{
		Context: ctx,
		Query:   parseQue,
		ParsedQuery: pcontext.ParsedQuery{
			Root: ast.Parse(parseQue),
		},
	}

	// 在这里执行 que，并且写回响应
	_, err := exec.hdl.Handle(pctx)
	if err != nil {
		// 回写错误响应
		// 先返回系统错误
		errResp := packet.BuildErInternalError(err.Error())
		return conn.WritePacket(packet.BuildErrRespPacket(errResp))
	}

	// TODO 如果是插入、更新、删除行为应该把影响行数和最后插入ID给传进去
	return conn.WritePacket(packet.BuildOKResp(packet.ServerStatusAutoCommit))
}

// parseQuery 获取sql语句
func (exec *StmtCloseExecutor) parseQuery(stmtId int) string {
	return fmt.Sprintf("EXECUTE stmt%d 1", stmtId)
	//return fmt.Sprintf("DEALLOCATE PREPARE stmt%d", stmtId)
}

// stmtId 获取对应prepare ID
func (exec *StmtCloseExecutor) parseStmtId(payload []byte) int {
	// 第一个字节是 cmd
	var stmtId uint32

	reader := bytes.NewReader(payload[1:5])

	if err := binary.Read(reader, binary.LittleEndian, &stmtId); err != nil {
		return 0
	}
	return int(stmtId)
}

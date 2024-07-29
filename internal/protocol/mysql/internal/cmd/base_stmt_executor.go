package cmd

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"log"
	"strings"
	"sync/atomic"

	"github.com/ecodeclub/ekit/slice"
	"github.com/ecodeclub/ekit/syncx"
	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/packet"
)

type BaseStmtExecutor struct {
	*BaseExecutor
	stmtIDGenerator  atomic.Uint32
	stmtID2NumParams syncx.Map[uint32, uint64]
}

func NewBaseStmtExecutor(base *BaseExecutor) *BaseStmtExecutor {
	return &BaseStmtExecutor{BaseExecutor: base}
}

// func (e *BaseStmtExecutor) cleanQuery(query string) string {
// 	return strings.ReplaceAll(query, "?", "'?'")
// }

// parseStmtID 获取对应prepare stmt id
func (e *BaseStmtExecutor) parseStmtID(payload []byte) uint32 {
	var stmtId uint32
	// 第一个字节是 cmd
	reader := bytes.NewReader(payload[1:5])
	if err := binary.Read(reader, binary.LittleEndian, &stmtId); err != nil {
		return 0
	}
	return stmtId
}

func (e *BaseStmtExecutor) parseArgs(stmtID uint32, payload []byte) ([]any, error) {
	var req packet.ExecuteStmtRequest
	numParams, ok := e.loadNumParams(stmtID)
	log.Printf("loadNumParams stmtID = %d, numParams = %d", stmtID, numParams)
	if !ok {
		return nil, fmt.Errorf("failed to load num params")
	}
	if err := req.Parse(numParams, payload); err != nil {
		return nil, err
	}
	return slice.Map(req.Parameters, func(idx int, src packet.ExecuteStmtRequestParameter) any {
		log.Printf("get execute params[%d] = %#v\n", idx, src)
		return src.Value
	}), nil
}

// generatePrepareStmtSQL 获取创建prepare的sql语句
func (e *BaseStmtExecutor) generatePrepareStmtSQL(stmtId uint32, query string) string {
	return fmt.Sprintf("PREPARE stmt%d FROM '%s'", stmtId, query)
}

// generateExecuteStmtSQL 获取执行prepare的sql语句
func (e *BaseStmtExecutor) generateExecuteStmtSQL(stmtId uint32) string {
	return fmt.Sprintf("EXECUTE stmt%d", stmtId)
}

// generateDeallocatePrepareStmtSQL 获取关闭prepare的sql语句
func (e *BaseStmtExecutor) generateDeallocatePrepareStmtSQL(stmtId uint32) string {
	return fmt.Sprintf("DEALLOCATE PREPARE stmt%d", stmtId)
}

func (e *BaseStmtExecutor) generateStmtID() uint32 {
	return e.stmtIDGenerator.Add(1)
}

func (e *BaseStmtExecutor) storeNumParams(stmtID uint32, query string) uint64 {
	// TODO: query中只根据?来判定参数个数有点弱, 因为用户可以传递 `col_name` = '?' | '__?'
	numParams := uint64(strings.Count(query, "?"))
	e.stmtID2NumParams.Store(stmtID, numParams)
	return numParams
}

func (e *BaseStmtExecutor) loadNumParams(stmtID uint32) (uint64, bool) {
	return e.stmtID2NumParams.Load(stmtID)
}

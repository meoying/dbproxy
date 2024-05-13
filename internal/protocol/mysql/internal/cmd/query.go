package cmd

import (
	"gitee.com/meoying/dbproxy/internal/protocol/mysql/internal/packet"
	"gitee.com/meoying/dbproxy/internal/protocol/mysql/internal/query"
)

var _ Executor = &QueryExecutor{}

type QueryExecutor struct {
}

// Exec
// Query 命令的 payload 格式在
// https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_query.html
func (exec *QueryExecutor) Exec(ctx *Context, payload []byte) ([]byte, error) {
	// 获取 params 的值
	//return nil, nil
	return exec.resp([]string{"id", "name"}, [][]any{{"1", "小李"}, {"1", "小明"}})
}

// resp 根据执行结果返回转换成对应的格式并返回
// response 的 text_resultset的格式在
// https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_query_response_text_resultset.html
func (exec *QueryExecutor) resp(cols []string, rows [][]any) ([]byte, error) {
	// text_resultset 由两部分组成
	// the column definitions (a.k.a. the metadata) 字段含义
	// the actual rows 实际行数据

	var p []byte

	// 写入字段数量
	p = packet.EncodeIntLenenc(uint64(len(cols)))
	// 写入字段描述包
	for _, c := range cols {
		p = append(p, packet.BuildColumnDefinitionPacket(c)...)
	}
	p = append(p, packet.BuildEOFPacket()...)

	// 写入真实每行数据
	for _, row := range rows {
		for _, v := range row {
			p = append(p, packet.BuildRowPacket(v)...)
		}
	}
	p = append(p, packet.BuildEOFPacket()...)

	return nil, nil
}

func (exec *QueryExecutor) paramIsNull(bitMap []byte, idx uint64) bool {
	// 在第几个字节
	bs := bitMap[idx/8]
	// 在该字节的哪个位置
	bsIdx := idx % 8
	return (bs & (1 << bsIdx)) > 0
}

func (exec *QueryExecutor) parseQuery(ctx *Context, payload []byte) query.Query {
	// 第一个字节是 cmd
	payload = payload[1:]
	// 参数个数，我在测试的时候，带参数的查询，走的是 prepare statement 那条路
	//var paramCnt uint64
	//if ctx.CapabilityFlags.Has(flags.ClientQueryAttributes) {
	//	// 参数个数编码占据的字节数量
	//	var bytes int
	//	paramCnt, bytes = packet.ReadEncodedLength(payload)
	//	payload = payload[bytes:]
	//	// 参数集合的个数
	//	_, psBytes := packet.ReadEncodedLength(payload)
	//	payload = payload[psBytes:]
	//}
	// 标记参数是否为 NULL
	//var nullBitMap []byte
	// 当前 mysql 协议里面，它永远是 1
	//var newParamsBindFlag uint8
	//if paramCnt > 0 {
	//	nullBitMapLen := (paramCnt + 7) / 8
	//	nullBitMap = payload[:nullBitMapLen]
	//	payload = payload[nullBitMapLen:]
	//	newParamsBindFlag = payload[0]
	//	payload = payload[1:]
	//}
	//params := make([]query.Param, 0, paramCnt)

	// 目前 mysql newParamsBindFlag 永远 > 0
	//if newParamsBindFlag > 0 {
	//	// 这个用来解析类型信息，比特是 1000 0000 0000 0000
	//	const signedBit uint16 = 1 << 15
	//	for i := uint64(0); i < paramCnt; i++ {
	//		// ^signedBit = 0111 1111 1111 1111
	//		typ := binary.LittleEndian.Uint16(payload[0:2]) & (^signedBit)
	//		// 最高的一位，标记这个是不是一个 unsigned 类型
	//		isUnsigned := binary.LittleEndian.Uint16(payload[0:2])&signedBit > 0
	//		payload = payload[2:]
	//		nameLen, nameLenBytes := packet.ReadEncodedLength(payload)
	//		payload = payload[nameLenBytes:]
	//		name := payload[:nameLen]
	//		payload = payload[nameLenBytes:]
	//		params = append(params, query.Param{
	//			IsNull:     exec.paramIsNull(nullBitMap, i),
	//			Type:       query.MySQLType(typ),
	//			IsUnsigned: isUnsigned,
	//			Name:       string(name),
	//		})
	//	}
	//}

	//for i := uint64(0); i < paramCnt; i++ {
	//	// NULL 则不需要处理
	//	if params[i].IsNull {
	//		continue
	//	}
	//	typ := params[i].Type
	//	// bytesCnt 是用来记录 length 多长的字节数
	//	// 在小字段里面，是 0
	//	// 大字段则不确定
	//	length, bytesCnt := packet.MySQLTypeLength(typ, payload)
	//	payload = payload[bytesCnt:]
	//	params[i].ValueBytes = payload[0:length]
	//	payload = payload[length:]
	//}

	sql := string(payload)
	return query.Query{
		SQL: sql,
	}
}

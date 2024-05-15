package packet

import (
	"encoding/binary"
	"fmt"
)

// 构造返回给客户端响应的 packet

// BuildErrRespPacket 构造一个错误响应给客户端
func BuildErrRespPacket(err ErrorResp) []byte {
	// 头部四个字节保留
	res := make([]byte, 4, 13+len(err.msg))
	// 固定 0xFF 代表错误
	res = append(res, 0xFF)
	// 错误码
	res = binary.LittleEndian.AppendUint16(res, err.code)
	// 我们是必然支持 CLIENT_PROTOCOL_41，所以要加 state 相关字段
	// 固定的 # 作为分隔符
	res = append(res, '#')
	res = append(res, err.state...)

	// 最后是人可读的错误信息
	res = append(res, err.msg...)
	return res
}

func BuildOKResp(status SeverStatus) []byte {
	// 头部的四个字节保留，不需要填充
	res := make([]byte, 4, 11)
	// 0 代表OK响应
	res = append(res, 0)
	// 0 影响行数
	res = append(res, 0)
	// 0 last_insert_id
	res = append(res, 0)
	// 服务器状态
	res = binary.LittleEndian.AppendUint16(res, status.AsUint16())
	// warning number 0 0
	res = append(res, 0, 0)
	return res
}

// BuildEOFPacket 生成一个结束符包
// https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_basic_eof_packet.html
func BuildEOFPacket() []byte {
	// 头部的四个字节保留，不需要填充
	res := make([]byte, 4, 9)
	// 代表eof包
	res = append(res, 0xfe)
	// 00 00代表没有警告
	res = append(res, []byte{0x00, 0x00}...)
	// 22 00 代表服务状态
	res = append(res, []byte{0x22, 0x00}...)
	return res
}

// BuildColumnDefinitionPacket 构建字段描述包
// https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_query_response_text_resultset_column_definition.html
func BuildColumnDefinitionPacket(col string) []byte {
	// 减少切片扩容
	p := make([]byte, 4, 32)

	// catalog string<lenenc> 目录
	p = append(p, EncodeStringLenenc("def")...)
	// schema string<lenenc> 数据库
	p = append(p, EncodeStringLenenc("test")...)
	// table string<lenenc> 虚拟数据表名
	p = append(p, EncodeStringLenenc("users")...)
	// orgTable string<lenenc> 物理数据表名
	p = append(p, EncodeStringLenenc("users")...)
	// name string<lenenc> 虚拟字段名
	p = append(p, EncodeStringLenenc(col)...)
	// orgName string<lenenc> 物理字段名
	p = append(p, EncodeStringLenenc(col)...)
	// 固定长度
	p = append(p, 0x0c)
	// character_set int<2> 编码，先固定为utf8mb3_general_ci
	p = append(p, UintLengthEncode(uint32(33), 2)...)
	// column_length int<4> 字段长度
	p = append(p, UintLengthEncode(uint32(len(col)), 4)...)
	// type int<1> 字段类型
	p = append(p, 3)
	// flags int<2> 标志
	p = append(p, UintLengthEncode(0, 2)...)
	// decimals int<1> 小数点
	p = append(p, 0)

	// 填充结束包
	p = append(p, 0, 0)

	return p
}

// BuildRowPacket 构建查询结果行字段包
// https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_query_response_text_resultset_row.html
func BuildRowPacket(value any) []byte {
	// 字段值为null 默认返回0xFB
	if value == nil {
		return []byte{0x00, 0x00, 0x00, 0x00, 0xFB}
	}
	// 字段值 string<lenenc>
	data := fmt.Sprintf("%v", value)
	return EncodeStringLenenc(data)
}

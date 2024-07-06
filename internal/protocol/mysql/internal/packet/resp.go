package packet

import (
	"encoding/binary"
	"github.com/meoying/dbproxy/internal/datasource/column"
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
func BuildColumnDefinitionPacket(col column.Column, charset uint32) []byte {
	// 减少切片扩容
	p := make([]byte, 4, 32)

	// catalog string<lenenc> 目录
	p = append(p, EncodeStringLenenc("def")...)
	// 这部分暂时用不到，所以全部写死
	// schema string<lenenc> 数据库
	p = append(p, EncodeStringLenenc("unsupported")...)
	// table string<lenenc> 虚拟数据表名
	p = append(p, EncodeStringLenenc("unsupported")...)
	// orgTable string<lenenc> 物理数据表名
	p = append(p, EncodeStringLenenc("unsupported")...)
	// name string<lenenc> 虚拟字段名
	p = append(p, EncodeStringLenenc(col.Name())...)
	// orgName string<lenenc> 物理字段名
	p = append(p, EncodeStringLenenc(col.Name())...)
	// 固定长度
	p = append(p, 0x0c)
	// character_set int<2> 编码
	p = append(p, UintLengthEncode(charset, 2)...)
	// column_length int<4> 字段类型最大长度
	p = append(p, UintLengthEncode(getMysqlTypeMaxLength(col.DatabaseTypeName()), 4)...)
	// type int<1> 字段类型
	p = append(p, uint16ToBytes(mapMySQLTypeToEnum(col.DatabaseTypeName()))...)
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
func BuildRowPacket(values ...any) []byte {
	// TODO 没有想到什么好的方法去判断any的类型，因为scan一定要指针，很难去转字符串
	// 减少切片扩容
	//return []byte{0x00, 0x00, 0x00, 0x00, 0x01, 0x31, 0x03, 0x54, 0x6f, 0x6d}
	p := make([]byte, 4, 20)
	for _, v := range values {
		// 字段值为null 默认返回0xFB
		data := *(v.(*[]byte))
		if data == nil {
			p = append(p, 0xFB)
		} else {
			// 字段值 string<lenenc>，由于row.Scan一定是指针，所以这里必定是*any指针，要取值，不然转字符串会返回16进制的地址
			p = append(p, EncodeStringLenenc(string(data))...)
		}
	}

	return p
}

// BuildBinaryRowPacket
// https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_binary_resultset.html#sect_protocol_binary_resultset_row
func BuildBinaryRowPacket(values ...any) []byte {
	p := make([]byte, 4, 20)

	// header
	p = append(p, 0)

	// null_bitmap TODO 暂定不会有null的情况，后续再优化NULL bitmap, length= (column_count + 7 + 2) / 8
	p = append(p, 0)

	// TODO 暂定先判断类型，后续要根据每个字段的类型去返回不同长度的包数据
	for key, val := range values {
		data := *(val.(*[]byte))
		if key == 0 {
			p = append(p, []byte{0x01, 0x00, 0x00, 0x00}...)
		} else {
			p = append(p, EncodeStringLenenc(string(data))...)
		}
	}

	return p
}

// getMysqlTypeMaxLength 获取字段类型最大长度
func getMysqlTypeMaxLength(dataType string) uint32 {
	// TODO 目前为了跑通流程先用着需要的，后续要继续补充所有类型
	switch dataType {
	case "INT":
		return MySqlMaxLengthInt
	case "BIGINT":
		return MySqlMaxLengthBigInt
	case "VARCHAR":
		return MySqlMaxLengthVarChar
	default:
		return 0
	}
}

// mapMySQLTypeToEnum 字段类型转字段枚举
func mapMySQLTypeToEnum(dataType string) uint16 {
	switch dataType {
	case "TINYINT":
		return uint16(MySQLTypeTiny)
	case "SMALLINT":
		return uint16(MySQLTypeShort)
	case "MEDIUMINT":
		return uint16(MySQLTypeInt24)
	case "INT":
		return uint16(MySQLTypeLong)
	case "BIGINT":
		return uint16(MySQLTypeLongLong)
	case "FLOAT":
		return uint16(MySQLTypeFloat)
	case "DOUBLE":
		return uint16(MySQLTypeDouble)
	case "DECIMAL":
		return uint16(MySQLTypeNewDecimal)
	case "CHAR":
		return uint16(MySQLTypeString)
	case "VARCHAR":
		return uint16(MySQLTypeVarString)
	case "TEXT":
		return uint16(MySQLTypeBlob)
	case "ENUM":
		return uint16(MySQLTypeString)
	case "SET":
		return uint16(MySQLTypeString)
	case "BINARY":
		return uint16(MySQLTypeString)
	case "VARBINARY":
		return uint16(MySQLTypeVarString)
	case "JSON":
		return uint16(MySQLTypeJSON)
	case "BIT":
		return uint16(MySQLTypeBit)
	case "DATE":
		return uint16(MySQLTypeDate)
	case "DATETIME":
		return uint16(MySQLTypeDatetime)
	case "TIMESTAMP":
		return uint16(MySQLTypeTimestamp)
	case "TIME":
		return uint16(MySQLTypeTime)
	case "YEAR":
		return uint16(MySQLTypeYear)
	case "GEOMETRY":
		return uint16(MySQLTypeGeometry)
	case "BLOB":
		return uint16(MySQLTypeBlob)
	default:
		return uint16(MySQLTypeVarString) // 未知类型
	}
}

// BuildStmtPacket 构建预处理响应包
// https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_stmt_prepare.html
func BuildStmtPacket(stmtId int, countCol int, countParam int) []byte {
	res := make([]byte, 4, 20)

	// status int<1>
	res = append(res, 0)

	// statement_id int<4>
	res = append(res, UintLengthEncode(uint32(stmtId), 4)...)

	// num_columns int<2>
	res = append(res, UintLengthEncode(uint32(countCol), 2)...)

	// num_params int<2>
	res = append(res, UintLengthEncode(uint32(countParam), 2)...)

	// reserved_1 int<1>
	res = append(res, 0)

	// warning_count int<2>
	res = append(res, 0, 0)

	return res
}

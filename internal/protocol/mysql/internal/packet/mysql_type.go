package packet

import "gitee.com/meoying/dbproxy/internal/protocol/mysql/internal/query"

// MySQLTypeLength 计算该类型在 packet 里面占据的长度
// 第一个是类型长度
// 第二个是记录类型长度的头部长度。
func MySQLTypeLength(typ query.MySQLType, payload []byte) (uint64, int) {
	switch typ {
	case query.MySQLTypeBool, query.MySQLTypeTiny:
		return 1, 0
	case query.MySQLTypeShort:
		return 2, 0
	case query.MySQLTypeFloat, query.MySQLTypeLong:
		return 4, 0
	case query.MySQLTypeDouble, query.MySQLTypeLongLong:
		return 8, 0
	default:
		return ReadEncodedLength(payload)
	}
}

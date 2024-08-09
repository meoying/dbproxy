package builder

import (
	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/packet/encoding"
)

// ColumnDefinition41Packet 构建字段描述包
type ColumnDefinition41Packet struct {
	Catalog      string
	Schema       string
	Table        string
	OrgTable     string
	Name         string
	OrgName      string
	CharacterSet uint32 // Character Set
	ColumnLength uint32
	Type         byte
	Flags        uint16
	Decimals     byte
}

// Build
// https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_query_response_text_resultset_column_definition.html
func (b *ColumnDefinition41Packet) Build() []byte {
	// 减少切片扩容
	p := make([]byte, 4, 32)

	// string<lenenc>	catalog 目录 The catalog used. Currently, always "def"
	p = append(p, encoding.LengthEncodeString(b.Catalog)...)

	// 这部分暂时用不到，所以全部写死
	// string<lenenc>   schema 数据库
	p = append(p, encoding.LengthEncodeString(b.Schema)...)

	// string<lenenc>	table 虚拟数据表名
	p = append(p, encoding.LengthEncodeString(b.Table)...)

	// string<lenenc>	org_table 物理数据表名
	p = append(p, encoding.LengthEncodeString(b.OrgTable)...)

	// string<lenenc>	name 虚拟字段名
	p = append(p, encoding.LengthEncodeString(b.Name)...)

	// string<lenenc>	org_name 物理字段名
	p = append(p, encoding.LengthEncodeString(b.OrgName)...)

	// int<lenenc>	length of fixed length fields 固定长度
	p = append(p, 0x0c)

	// int<2>	character_set the column character set as defined in Character Set 编码
	p = append(p, encoding.FixedLengthInteger(uint64(b.CharacterSet), 2)...)

	// int<4>	column_length	maximum length of the field 字段类型最大长度
	p = append(p, encoding.FixedLengthInteger(uint64(b.ColumnLength), 4)...)

	// int<1>	type 字段类型的数字表示 详见 mysql_type.go
	p = append(p, encoding.FixedLengthInteger(uint64(b.Type), 1)...)

	// int<2>	flags	字段定义标志
	// TODO: [改进] 需要从col获取相关信息来设置准确值
	// https://dev.mysql.com/doc/dev/mysql-server/latest/group__group__cs__column__definition__flags.html
	p = append(p, encoding.FixedLengthInteger(uint64(b.Flags), 2)...)

	// int<1>	decimals	max shown decimal digits:
	//		0x00 for integers and static strings
	//		0x1f for dynamic strings, double, float
	//		0x00 to 0x51 for decimals
	// TODO: 这里有问题,要根据不同数据类型返回不同的内容
	p = append(p, b.Decimals)

	// 填充结束包
	// p = append(p, 0, 0)

	return p
}

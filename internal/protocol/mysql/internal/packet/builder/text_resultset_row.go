package builder

import "github.com/meoying/dbproxy/internal/protocol/mysql/internal/packet/encoding"

// TextResultSetRowPacket 文本协议结果集行构造器
type TextResultSetRowPacket struct {
	values []any
}

// Build 构建
// https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_query_response_text_resultset_row.html
func (b *TextResultSetRowPacket) Build() []byte {
	// TODO 没有想到什么好的方法去判断any的类型，因为scan一定要指针，很难去转字符串
	p := make([]byte, 4, 20)
	for _, v := range b.values {
		data := *(v.(*[]byte))
		if data == nil {
			// 字段值为null 默认返回0xFB
			p = append(p, 0xFB)
		} else {
			// 字段值 string<lenenc>，由于row.Scan一定是指针，所以这里必定是*any指针，要取值，不然转字符串会返回16进制的地址
			p = append(p, encoding.LengthEncodeString(string(data))...)
		}
	}
	return p
}

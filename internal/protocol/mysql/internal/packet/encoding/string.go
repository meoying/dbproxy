package encoding

// LengthEncodeString 对字符串进行 string<lenenc> 编码
// https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_basic_dt_strings.html#sect_protocol_basic_dt_string_le
func LengthEncodeString(str string) []byte {
	// 将字符串的长度以 int<lenenc> 编码形式作为前缀与字符串内容拼接
	return append(LengthEncodeInteger(uint64(len(str))), []byte(str)...)
}

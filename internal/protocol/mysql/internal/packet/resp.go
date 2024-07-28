package packet

import (
	"bytes"
	"database/sql"
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"reflect"
	"strconv"
	"strings"
	"time"
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
func BuildEOFPacket(status SeverStatus) []byte {
	// 头部的四个字节保留，不需要填充
	res := make([]byte, 4, 9)
	// 代表eof包
	res = append(res, 0xfe)
	// 00 00代表没有警告
	res = append(res, []byte{0x00, 0x00}...)
	// 服务器状态
	res = binary.LittleEndian.AppendUint16(res, status.AsUint16())
	return res
}

type ColumnType interface {
	Name() string
	DatabaseTypeName() string
}

// BuildColumnDefinitionPacket 构建字段描述包
// https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_query_response_text_resultset_column_definition.html
func BuildColumnDefinitionPacket(col ColumnType, charset uint32) []byte {
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

// BuildTextResultsetRowRespPacket 构建查询结果行字段包
// https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_query_response_text_resultset_row.html
func BuildTextResultsetRowRespPacket(values []any, cols []ColumnType) []byte {
	// TODO 没有想到什么好的方法去判断any的类型，因为scan一定要指针，很难去转字符串
	// 减少切片扩容
	// return []byte{0x00, 0x00, 0x00, 0x00, 0x01, 0x31, 0x03, 0x54, 0x6f, 0x6d}
	p := make([]byte, 4, 20)
	for _, v := range values {
		// 字段值为null 默认返回0xFB
		data := *(v.(*[]byte))
		// data := convertToBytes(v)
		if data == nil {
			p = append(p, 0xFB)
		} else {
			// 字段值 string<lenenc>，由于row.Scan一定是指针，所以这里必定是*any指针，要取值，不然转字符串会返回16进制的地址
			p = append(p, EncodeStringLenenc(string(data))...)
		}
	}

	return p
}

// p := make([]byte, 4, 20)
//
// // header
// p = append(p, 0)
//
// // null_bitmap TODO 暂定不会有null的情况，后续再优化NULL bitmap, length= (column_count + 7 + 2) / 8
// p = append(p, 0)
//
// // TODO 暂定先判断类型，后续要根据每个字段的类型去返回不同长度的包数据
// for key, val := range values {
// 	data := *(val.(*[]byte))
// 	if key == 0 {
// 		p = append(p, []byte{0x01, 0x00, 0x00, 0x00}...)
// 	} else {
// 		p = append(p, EncodeStringLenenc(string(data))...)
// 	}
// }

// BuildBinaryResultsetRowRespPacket
// https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_binary_resultset.html#sect_protocol_binary_resultset_row
func BuildBinaryResultsetRowRespPacket(values []any, cols []ColumnType) []byte {
	log.Printf("BuildBinaryResultsetRowRespPacket values = %#v\n", values)
	// Calculate the length of the NULL bitmap
	nullBitmapLen := (len(values) + 7 + 2) / 8

	// Initialize the NULL bitmap
	nullBitmap := make([]byte, nullBitmapLen)

	// Build the row values
	var buf bytes.Buffer
	for i, val := range values {
		//
		ok, err := writeBinaryValue2(&buf, val, cols[i])
		if err != nil {
			// handle error or panic
			log.Printf(">>>>>>>>>>>> ERROR ERROR >>>>>>>> %#v\n", err)
		}
		// 写入失败但没有error 就是 NULL的意思
		if !ok {
			// Set the NULL bit in the bitmap
			bytePos := (i + 2) / 8
			bitPos := (i + 2) % 8
			nullBitmap[bytePos] |= 1 << bitPos
		}
		// else {
		// 	// Append the value to the row bytes
		// 	err := writeBinaryValue(&buf, val)
		// 	if err != nil {
		// 		// handle error or panic
		// 		log.Printf(">>>>>>>>>>>> ERROR ERROR >>>>>>>> %#v\n", err)
		// 	}
		// }
	}
	row := buf.Bytes()
	p := make([]byte, 4, 4+1+len(nullBitmap)+len(row))
	log.Printf("packet  len=%#v, cap=%#v\n", len(p), cap(p))
	// header
	p = append(p, 0x00)
	log.Printf("write Header p = %#v\n", p[4:])

	// null_bitmap
	p = append(p, nullBitmap...)
	log.Printf("write null bitmap p = %#v\n", p[4:])
	// values
	p = append(p, row...)
	log.Printf("write rows p = %#v\n", p[4:])
	return p
}

// writeBinaryValue2
// bool 表示 写入 buf 成功
// error 表示 转换、写入过程中的错误
// NULL = false, nil, 没写入buf,没错误
func writeBinaryValue2(buf *bytes.Buffer, value any, col ColumnType) (bool, error) {
	if value == nil {
		return false, nil
	}

	// 确保 val 是 *[]byte 类型
	bytesPtr, ok := value.(*[]byte)
	if !ok {
		return false, fmt.Errorf("val类型非法: %T", value)
	}

	if bytesPtr == nil {
		return false, nil
	}

	// 解引用 *[]byte 得到 []byte
	bytesVal := *bytesPtr

	if bytesVal == nil {
		return false, nil
	}
	// var val any

	// 转化为特定的类型
	switch col.DatabaseTypeName() {
	case "TINYINT":
		// 将 []byte 转换为 int8 类型
		v, err := strconv.ParseInt(string(bytesVal), 10, 8)
		if err != nil {
			return false, err
		}
		// val = int8(v)
		err = binary.Write(buf, binary.LittleEndian, int8(v))
		if err != nil {
			return false, err
		}
		return true, nil
	case "SMALLINT", "YEAR":
		// 将 []byte 转换为 int16 类型
		v, err := strconv.ParseInt(string(bytesVal), 10, 16)
		if err != nil {
			return false, err
		}
		// val = int16(v)
		err = binary.Write(buf, binary.LittleEndian, int16(v))
		if err != nil {
			return false, err
		}
		return true, nil
	case "INT", "MEDIUMINT":
		// 将 []byte 转换为 int32 类型
		s := string(bytesVal)
		log.Printf("INT, MEDIUMINT = %s\n", s)
		v, err := strconv.ParseInt(s, 10, 32)
		if err != nil {
			return false, err
		}
		// val = int32(v)
		err = binary.Write(buf, binary.LittleEndian, int32(v))
		if err != nil {
			return false, err
		}
		return true, nil
	case "BIGINT":
		// 将 []byte 转换为 int64 类型
		v, err := strconv.ParseInt(string(bytesVal), 10, 64)
		if err != nil {
			return false, err
		}
		// val = v
		err = binary.Write(buf, binary.LittleEndian, v)
		if err != nil {
			return false, err
		}
		return true, nil
	case "FLOAT":
		f, err := strconv.ParseFloat(string(bytesVal), 32)
		if err != nil {
			return false, err
		}
		// val = float32(f)
		err = binary.Write(buf, binary.LittleEndian, float32(f))
		if err != nil {
			return false, err
		}
		return true, nil
	case "DOUBLE":
		f, err := strconv.ParseFloat(string(bytesVal), 64)
		if err != nil {
			return false, err
		}
		// val = f
		err = binary.Write(buf, binary.LittleEndian, f)
		if err != nil {
			return false, err
		}
		return true, nil
	case "DECIMAL", "CHAR", "VARCHAR", "TEXT", "ENUM", "SET", "BINARY", "VARBINARY", "JSON", "BIT", "BLOB", "GEOMETRY":
		// val = string(bytesVal)
		_, err := buf.Write(EncodeStringLenenc(string(bytesVal)))
		if err != nil {
			return false, err
		}
		return true, nil
	case "DATE", "DATETIME", "TIMESTAMP":
		log.Printf("<<<<<<>>>>> val = %#v, type = %s\n", bytesVal, col.DatabaseTypeName())
		v, _, err := parseTime(bytesVal, col.DatabaseTypeName())
		if err != nil {
			return false, err
		}
		// val = v

		year, month, day := v.Date()
		hour, minute, second := v.Clock()
		nanosecond := v.Nanosecond()

		// 将纳秒转换为微秒
		microsecond := nanosecond / int(time.Microsecond)

		for _, field := range []any{
			int8(11), // 长度
			int16(year),
			int8(month),
			int8(day),
			int8(hour),
			int8(minute),
			int8(second),
			int32(microsecond),
		} {
			if err := binary.Write(buf, binary.LittleEndian, field); err != nil {
				return false, err
			}
		}
		return true, nil

	case "TIME":
		v, _, err := parseTime(bytesVal, col.DatabaseTypeName())
		if err != nil {
			return false, err
		}

		isNegative := 0
		if strings.HasPrefix(string(bytesVal), "-") {
			isNegative = 1
		}

		hour := v.Hour()

		days := hour / 24
		hours := hour % 24

		minute := v.Minute()
		second := v.Second()
		microsecond := v.Nanosecond() / 1000

		for _, field := range []any{
			int8(12),         // 长度
			int8(isNegative), // is_negative	1 if minus, 0 for plus
			int32(days),
			int8(hours),
			int8(minute),
			int8(second),
			int32(microsecond),
		} {
			if err := binary.Write(buf, binary.LittleEndian, field); err != nil {
				return false, err
			}
		}
		return true, nil
	default:
		return false, errors.New("未支持的数据库数据类型")
	}

	// 写入buffer
	// switch vv := val.(type) {
	// case int8, int16, int32, int64, float32, float64:
	// 	log.Printf("write %T, %#v\n", vv, vv)
	// 	err := binary.Write(buf, binary.LittleEndian, vv)
	// 	if err != nil {
	// 		return false, err
	// 	}
	// 	return true, nil
	// case sql.NullInt64:
	// 	if vv.Valid {
	// 		err := binary.Write(buf, binary.LittleEndian, vv.Int64)
	// 		if err != nil {
	// 			return false, err
	// 		}
	// 		return true, nil
	// 	}
	// 	return false, nil
	// case bool:
	// 	var boolValue byte
	// 	if vv {
	// 		boolValue = 1
	// 	}
	// 	log.Printf("write %T, %#v\n", vv, vv)
	// 	err := buf.WriteByte(boolValue)
	// 	if err != nil {
	// 		return false, err
	// 	}
	// 	return true, nil
	// case []byte:
	// 	log.Printf("write %T, %#v\n", vv, vv)
	// 	_, err := buf.Write(EncodeStringLenenc(string(vv)))
	// 	if err != nil {
	// 		return false, err
	// 	}
	// 	return true, nil
	// case string:
	// 	log.Printf("write %T, %#v\n", vv, vv)
	// 	_, err := buf.Write(EncodeStringLenenc(vv))
	// 	if err != nil {
	// 		return false, err
	// 	}
	// 	return true, nil
	// case time.Time:
	//
	// 	if col.DatabaseTypeName() == "TIME" {
	//
	// 		isNegative := 0
	// 		if strings.HasPrefix(string(bytesVal), "-") {
	// 			isNegative = 1
	// 		}
	//
	// 		hour := vv.Hour()
	//
	// 		days := hour / 24
	// 		hours := hour % 24
	//
	// 		minute := vv.Minute()
	// 		second := vv.Second()
	// 		microsecond := vv.Nanosecond() / 1000
	//
	// 		for _, field := range []any{
	// 			int8(12),         // 长度
	// 			int8(isNegative), // is_negative	1 if minus, 0 for plus
	// 			int32(days),
	// 			int8(hours),
	// 			int8(minute),
	// 			int8(second),
	// 			int32(microsecond),
	// 		} {
	// 			if err := binary.Write(buf, binary.LittleEndian, field); err != nil {
	// 				return false, err
	// 			}
	// 		}
	// 	} else {
	//
	// 		year, month, day := vv.Date()
	// 		hour, minute, second := vv.Clock()
	// 		nanosecond := vv.Nanosecond()
	//
	// 		// 将纳秒转换为微秒
	// 		microsecond := nanosecond / int(time.Microsecond)
	//
	// 		for _, field := range []any{
	// 			int8(11), // 长度
	// 			int16(year),
	// 			int8(month),
	// 			int8(day),
	// 			int8(hour),
	// 			int8(minute),
	// 			int8(second),
	// 			int32(microsecond),
	// 		} {
	// 			if err := binary.Write(buf, binary.LittleEndian, field); err != nil {
	// 				return false, err
	// 			}
	// 		}
	// 	}
	// 	return true, nil
	// default:
	// 	return false, fmt.Errorf("未支持的Go数据类型 %T", vv)
	// }

}

// 定义可能的日期格式
var formatMap = map[string][]string{
	"DATE":      {"2006-01-02"},
	"DATETIME":  {"2006-01-02 15:04:05", "2006-01-02 15:04"},
	"TIMESTAMP": {"2006-01-02 15:04:05", "2006-01-02 15:04"},
	"TIME":      {"15:04:05"},
}

// parseTime 解析字节切片中的日期时间字符串并返回 time.Time
func parseTime(data []byte, columnDatabaseType string) (time.Time, string, error) {
	log.Printf("<<<<<<>>>>> val = %#v, type = %s\n", data, columnDatabaseType)
	dateStr := string(bytes.TrimSpace(data))

	layouts, ok := formatMap[columnDatabaseType]
	if !ok {
		return time.Time{}, "", fmt.Errorf("unsupported column type: %s", columnDatabaseType)
	}

	for _, layout := range layouts {
		parsedTime, err := time.Parse(layout, dateStr)
		if err == nil {
			return parsedTime, layout, nil
		}
	}

	return time.Time{}, "", fmt.Errorf("cannot parse date: %s", dateStr)
}

// ConvertToBinaryProtocolValue 根据 col 中的类型信息将 val 转换为 mysql二进制协议值
// https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_binary_resultset.html#sect_protocol_binary_resultset_row_value
func ConvertToBinaryProtocolValue(value any, col *sql.ColumnType) (any, error) {
	log.Printf("ConvertToBinaryProtocolValue, name = %s, type = %T, val = %#v\n", col.Name(), value, value)

	if value == nil {
		return nil, nil
	}

	// 确保 val 是 *[]byte 类型
	bytesPtr, ok := value.(*[]byte)
	if !ok {
		return nil, fmt.Errorf("val类型非法: %T", value)
	}

	if bytesPtr == nil {
		return nil, nil
	}

	// 解引用 *[]byte 得到 []byte
	bytesVal := *bytesPtr

	if bytesVal == nil {
		return nil, nil
	}

	switch col.DatabaseTypeName() {
	case "TINYINT":
		// 将 []byte 转换为 int8 类型
		v, err := strconv.ParseInt(string(bytesVal), 10, 8)
		if err != nil {
			return nil, err
		}
		return int8(v), nil
	case "SMALLINT", "YEAR":
		// 将 []byte 转换为 int16 类型
		v, err := strconv.ParseInt(string(bytesVal), 10, 16)
		if err != nil {
			return nil, err
		}
		return int16(v), nil
	case "INT", "MEDIUMINT":
		// 将 []byte 转换为 int32 类型
		s := string(bytesVal)
		log.Printf("INT, MEDIUMINT = %s\n", s)
		v, err := strconv.ParseInt(s, 10, 32)
		if err != nil {
			return nil, err
		}
		return int32(v), nil
	case "BIGINT":
		// 将 []byte 转换为 int64 类型
		return strconv.ParseInt(string(bytesVal), 10, 64)
	case "FLOAT":
		f, err := strconv.ParseFloat(string(bytesVal), 32)
		if err != nil {
			return nil, err
		}
		return float32(f), nil
	case "DOUBLE":
		return strconv.ParseFloat(string(bytesVal), 64)
	case "DECIMAL", "CHAR", "VARCHAR", "TEXT", "ENUM", "SET", "BINARY", "VARBINARY", "JSON", "BIT", "BLOB", "GEOMETRY":
		return string(bytesVal), nil
	case "DATE", "DATETIME", "TIMESTAMP":
		return nil, nil
	case "TIME":
		return nil, nil
	default:
		return nil, errors.New("unsupported database type")
	}
}

func writeBinaryValue(buf *bytes.Buffer, value any) error {

	log.Printf("writeBinaryValue = %T, %#v\n", value, value)

	switch v := value.(type) {
	case int8, int16, int32, int64, float32, float64:
		log.Printf("write %T, %#v\n", v, v)
		return binary.Write(buf, binary.LittleEndian, v)
	case sql.NullInt64:
		if v.Valid {
			return writeBinaryValue(buf, v.Int64)
		}
		return nil
	case bool:
		var boolValue byte
		if v {
			boolValue = 1
		}
		log.Printf("write %T, %#v\n", v, v)
		return buf.WriteByte(boolValue)
	case []byte:
		log.Printf("write %T, %#v\n", v, v)
		_, err := buf.Write(EncodeStringLenenc(string(v)))
		return err
	case string:
		log.Printf("write %T, %#v\n", v, v)
		_, err := buf.Write(EncodeStringLenenc(v))
		return err
	default:
		return fmt.Errorf("未支持的列类型 %T", v)
	}
}

// convertToBytes 将任意类型的值转换为字符串
// TODO: 未使用 去掉
func convertToBytes(value any) []byte {
	log.Printf("ConsertValue = %#v, %T\n", value, value)
	if value == nil {
		return nil
	}
	v := reflect.ValueOf(value)
	kind := v.Kind()

	switch kind {
	case reflect.String:
		return []byte(v.String())
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return []byte(fmt.Sprintf("%d", v.Int()))
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return []byte(fmt.Sprintf("%d", v.Uint()))
	case reflect.Float32, reflect.Float64:
		return []byte(fmt.Sprintf("%f", v.Float()))
	case reflect.Bool:
		return []byte(fmt.Sprint(v.Bool()))
	case reflect.Slice, reflect.Array:
		if kind == reflect.Slice && v.Type().Elem().Kind() == reflect.Uint8 {
			// Special case for []byte
			return v.Bytes()
		}
		return []byte(fmt.Sprint(v.Interface()))
	case reflect.Map, reflect.Struct:
		switch v.Type().String() {
		case "sql.NullString":
			ns := value.(sql.NullString)
			if ns.Valid {
				return convertToBytes(ns.String)
			}
			return nil
		case "sql.NullByte":
			nb := value.(sql.NullByte)
			if nb.Valid {
				return convertToBytes(nb.Byte)
			}
			return nil
		case "sql.NullInt16":
			ni := value.(sql.NullInt16)
			if ni.Valid {
				return convertToBytes(ni.Int16)
			}
			return nil
		case "sql.NullInt32":
			ni := value.(sql.NullInt32)
			if ni.Valid {
				return convertToBytes(ni.Int32)
			}
			return nil
		case "sql.NullInt64":
			ni := value.(sql.NullInt64)
			if ni.Valid {
				return convertToBytes(ni.Int64)
			}
			return nil
		case "sql.NullFloat64":
			nf := value.(sql.NullFloat64)
			if nf.Valid {
				return convertToBytes(nf.Float64)
			}
			return nil
		case "sql.NullBool":
			nb := value.(sql.NullBool)
			if nb.Valid {
				return convertToBytes(nb.Bool)
			}
			return nil
		case "sql.NullTime":
			nt := value.(sql.NullTime)
			if nt.Valid {
				return convertToBytes(nt.Time)
			}
			return nil
		case "time.Time":
			// TODO: 时间转化问题
			// return []byte(v.Interface().(time.Time).UTC().Format(time.RFC3339))
			return []byte(v.Interface().(time.Time).Format(time.RFC3339))
			// return []byte(fmt.Sprint(v.Interface()))
		}
		return []byte(fmt.Sprint(v.Interface()))
	case reflect.Ptr:
		if v.IsNil() {
			return nil
		}
		return convertToBytes(v.Elem().Interface())
	case reflect.Interface:
		if v.IsNil() {
			return nil
		}
		return convertToBytes(v.Interface())
	case reflect.Complex64, reflect.Complex128:
		return []byte(fmt.Sprintf("%g", v.Complex()))
	default:
		return []byte(fmt.Sprint(v.Interface()))
	}
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

// BuildStmtPrepareRespPacket 构建预处理响应包
// https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_stmt_prepare.html
func BuildStmtPrepareRespPacket(stmtId, numColumns, numParams int) []byte {
	res := make([]byte, 4, 20)

	// status int<1>
	res = append(res, 0)

	// statement_id int<4>
	res = append(res, UintLengthEncode(uint32(stmtId), 4)...)

	// num_columns int<2>
	res = append(res, UintLengthEncode(uint32(numColumns), 2)...)

	// num_params int<2>
	res = append(res, UintLengthEncode(uint32(numParams), 2)...)

	// reserved_1 int<1>
	res = append(res, 0)

	// warning_count int<2>
	res = append(res, 0, 0)

	return res
}

// BuildStmtExecuteRespPacket 构建执行预处理响应包
// 主要用于查询语句, 插入、修改、删除语句用BuildOKResp, 错误用BuildErrRespPacket
// https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_com_stmt_execute_response.html
func BuildStmtExecuteRespPacket(stmtId, numColumns, numParams int) []byte {
	return nil
}

func EncodeBinaryProtocolResultsetRow(rows *sql.Rows) ([]byte, error) {
	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	var buf bytes.Buffer

	// 写入 packet_header
	if err := buf.WriteByte(0x00); err != nil {
		return nil, err
	}

	numFields := len(columns)
	nullBitmapLen := (numFields + 7 + 2) / 8
	nullBitmap := make([]byte, nullBitmapLen)
	values := make([]any, numFields)
	valuePtrs := make([]any, numFields)

	for rows.Next() {
		for i := range columns {
			valuePtrs[i] = &values[i]
		}

		if err := rows.Scan(valuePtrs...); err != nil {
			return nil, err
		}

		// 重置 nullBitmap
		for i := range nullBitmap {
			nullBitmap[i] = 0
		}

		for fieldPos, col := range values {
			if col == nil {
				bytePos := (fieldPos + 2) / 8
				bitPos := (fieldPos + 2) % 8
				nullBitmap[bytePos] |= 1 << bitPos
			}
		}

		if _, err := buf.Write(nullBitmap); err != nil {
			return nil, err
		}

		for _, col := range values {
			if col != nil {
				switch v := col.(type) {
				case int64:
					if err := binary.Write(&buf, binary.LittleEndian, v); err != nil {
						return nil, err
					}
				case float64:
					if err := binary.Write(&buf, binary.LittleEndian, v); err != nil {
						return nil, err
					}
				// case bool:
				// 	if err := buf.WriteByte(v); err != nil {
				// 		return nil, err
				// 	}
				case []byte:
					if _, err := buf.Write(v); err != nil {
						return nil, err
					}
				case string:
					if _, err := buf.Write([]byte(v)); err != nil {
						return nil, err
					}
				default:
					return nil, fmt.Errorf("unsupported column type %T", v)
				}
			}
		}
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

// func parseTime(data []byte) (time.Time, error) {
// 	layouts := []string{
// 		"2006-01-02",
// 		"2006-01-02 15:04",
// 		"2006-01-02 15:04:05",
// 		"15:04:05",
// 	}
//
// 	dateStr := string(bytes.TrimSpace(data))
// 	var parsedTime time.Time
// 	var err error
//
// 	for _, layout := range layouts {
// 		parsedTime, err = time.Parse(layout, dateStr)
// 		if err == nil {
// 			return parsedTime, nil
// 		}
// 	}
//
// 	return time.Time{}, fmt.Errorf("cannot parse date: %s", dateStr)
// }

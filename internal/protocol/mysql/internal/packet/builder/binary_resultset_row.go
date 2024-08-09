package builder

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"log"
	"strconv"
	"strings"
	"time"

	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/packet/encoding"
)

type BinaryResultSetRowPacket struct {
	values []any
	cols   []ColumnType
}

// Build
// https://dev.mysql.com/doc/dev/mysql-server/latest/page_protocol_binary_resultset.html#sect_protocol_binary_resultset_row
func (b *BinaryResultSetRowPacket) Build() ([]byte, error) {
	log.Printf("BuildBinaryResultsetRowRespPacket values = %#v\n", b.values)
	// Calculate the length of the NULL bitmap
	nullBitmapLen := (len(b.values) + 7 + 2) / 8

	// Initialize the NULL bitmap
	nullBitmap := make([]byte, nullBitmapLen)

	// Build the row values
	var buf bytes.Buffer
	for i, val := range b.values {
		//
		ok, err := writeBinaryValue(&buf, val, b.cols[i])

		if err != nil {
			// handle error or panic
			log.Printf("BuildBinaryResultsetRowRespPacket ERROR: %#v\n", err)
			return nil, err
		}

		// 写入失败但没有error 就是 NULL的意思
		if !ok {
			// Set the NULL bit in the bitmap
			bytePos := (i + 2) / 8
			bitPos := (i + 2) % 8
			nullBitmap[bytePos] |= 1 << bitPos
		}
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
	return p, nil
}

// writeBinaryValue
// bool 表示 写入buf
// error 表示 转换数据类型或者写入buf的过程中的错误
// 当 value = NULL 时 返回 false, nil 表示 没有写入buf且没错误
func writeBinaryValue(buf *bytes.Buffer, value any, col ColumnType) (bool, error) {
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
		_, err := buf.Write(encoding.LengthEncodeString(string(bytesVal)))
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

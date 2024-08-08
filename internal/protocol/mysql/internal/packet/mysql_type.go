package packet

// MySQLType MySQL 的数据类型
// https://dev.mysql.com/doc/dev/mysql-server/latest/field__types_8h.html#a69e798807026a0f7e12b1d6c72374854
type MySQLType uint16

const (
	MySQLTypeDecimal    MySQLType = 0
	MySQLTypeTiny       MySQLType = 1
	MySQLTypeShort      MySQLType = 2
	MySQLTypeLong       MySQLType = 3
	MySQLTypeFloat      MySQLType = 4
	MySQLTypeDouble     MySQLType = 5
	MySQLTypeNULL       MySQLType = 6
	MySQLTypeTimestamp  MySQLType = 7
	MySQLTypeLongLong   MySQLType = 8
	MySQLTypeInt24      MySQLType = 9
	MySQLTypeDate       MySQLType = 10
	MySQLTypeTime       MySQLType = 11
	MySQLTypeDatetime   MySQLType = 12
	MySQLTypeYear       MySQLType = 13
	MySQLTypeNewDate    MySQLType = 14 /**< Internal to MySQL. Not used in protocol */
	MySQLTypeVarchar    MySQLType = 15
	MySQLTypeBit        MySQLType = 16
	MySQLTypeTimestamp2 MySQLType = 17
	MySQLTypeDatetime2  MySQLType = 18 /**< Internal to MySQL. Not used in protocol */
	MySQLTypeTime2      MySQLType = 19 /**< Internal to MySQL. Not used in protocol */
	MySQLTypeTypedArray MySQLType = 20 /**< Used for replication only */
	MySQLTypeInvalid    MySQLType = 243
	MySQLTypeBool       MySQLType = 244 /**< Currently just a placeholder */
	MySQLTypeJSON       MySQLType = 245
	MySQLTypeNewDecimal MySQLType = 246
	MySQLTypeEnum       MySQLType = 247
	MySQLTypeSet        MySQLType = 248
	MySQLTypeTinyBlob   MySQLType = 249
	MySQLTypeMediumBlob MySQLType = 250
	MySQLTypeLongBlob   MySQLType = 251
	MySQLTypeBlob       MySQLType = 252
	MySQLTypeVarString  MySQLType = 253
	MySQLTypeString     MySQLType = 254
	MySQLTypeGeometry   MySQLType = 255
)

// GetMySQLType 根据 databaseTypeName 获取对应的 MySQLType
func GetMySQLType(databaseTypeName string) MySQLType {
	switch databaseTypeName {
	case "TINYINT":
		return MySQLTypeTiny
	case "SMALLINT":
		return MySQLTypeShort
	case "MEDIUMINT":
		return MySQLTypeInt24
	case "INT":
		return MySQLTypeLong
	case "BIGINT":
		return MySQLTypeLongLong
	case "FLOAT":
		return MySQLTypeFloat
	case "DOUBLE":
		return MySQLTypeDouble
	case "DECIMAL":
		return MySQLTypeNewDecimal
	case "CHAR":
		return MySQLTypeString
	case "VARCHAR":
		return MySQLTypeVarString
	case "TEXT":
		return MySQLTypeBlob
	case "ENUM":
		return MySQLTypeString
	case "SET":
		return MySQLTypeString
	case "BINARY":
		return MySQLTypeString
	case "VARBINARY":
		return MySQLTypeVarString
	case "JSON":
		return MySQLTypeJSON
	case "BIT":
		return MySQLTypeBit
	case "DATE":
		return MySQLTypeDate
	case "DATETIME":
		return MySQLTypeDatetime
	case "TIMESTAMP":
		return MySQLTypeTimestamp
	case "TIME":
		return MySQLTypeTime
	case "YEAR":
		return MySQLTypeYear
	case "GEOMETRY":
		return MySQLTypeGeometry
	case "BLOB":
		return MySQLTypeBlob
	default:
		return MySQLTypeVarString // 未知类型
	}
}

// 各字段类型的最大长度
const (
	MySqlMaxLengthTinyInt   uint32 = 4
	MySqlMaxLengthSmallInt  uint32 = 6
	MySqlMaxLengthMediumInt uint32 = 9
	MySqlMaxLengthInt       uint32 = 11
	MySqlMaxLengthBigInt    uint32 = 20
	MySqlMaxLengthVarChar   uint32 = 40
)

// GetMysqlTypeMaxLength 获取字段类型最大长度
func GetMysqlTypeMaxLength(databaseTypeName string) uint32 {
	// TODO 目前为了跑通流程先用着需要的，后续要继续补充所有类型
	switch databaseTypeName {
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

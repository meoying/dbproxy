package query

// MySQLType MySQL 的数据类型
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

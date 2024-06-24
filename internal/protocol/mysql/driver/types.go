//go:generate mockgen -source=./types.go -destination=mocks/driver.mock.go -package=mocks -typed
package driver

import (
	"database/sql/driver"
	"io"
)

// Driver 表示driver包中的[驱动]对象要实现的方法集合
// 注意: 常用的SQLite3包实现了 Driver 接口但未实现 DriverContext 接口
type Driver interface {
	driver.DriverContext
}

// Connector 表示driver包中的[连接器]对象要实现的方法集合
// 注意: 常用的SQLite3包未实现 Connector 接口
type Connector interface {
	driver.Connector
}

// Conn 表示driver包中的[连接]对象要实现的方法集合
// 注意: 常用的SQLite3包未实现下方io.Closer后面的接口
type Conn interface {
	driver.Pinger
	driver.ExecerContext
	driver.QueryerContext
	driver.Conn
	driver.ConnPrepareContext
	driver.ConnBeginTx
	io.Closer
	driver.SessionResetter
	driver.Validator
	driver.NamedValueChecker
}

// Rows 表示driver包中的[行]对象要实现的方法集合
// 注意: 常用的SQLite3包未实现下方io.Closer后面的接口
type Rows interface {
	driver.Rows
	driver.RowsColumnTypeScanType
	driver.RowsColumnTypeDatabaseTypeName
	driver.RowsColumnTypeNullable
	io.Closer
	driver.RowsColumnTypePrecisionScale
	driver.RowsNextResultSet
}

// Stmt 表示driver包中的[语句]对象要实现的方法集合
// 注意: 常用的SQLite3包未实现下方io.Closer后面的接口
type Stmt interface {
	driver.Stmt
	driver.StmtExecContext
	driver.StmtQueryContext
	io.Closer
	driver.NamedValueChecker
	driver.ColumnConverter
}

// Tx 表示driver包中的[事务]对象要实现的方法集合
type Tx interface {
	driver.Tx
}

// Result 表示driver包中的[结果]对象要实现的方法集合
type Result interface {
	driver.Result
}

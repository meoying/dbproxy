//go:generate mockgen -source=./types.go -destination=mocks/types.mock.go -package=mocks -typed
package mysql

import (
	"database/sql/driver"
	"io"
)

// Driver 接口表示mysql driver中表示驱动的对象上实现的接口集合
type Driver interface {
	driver.Driver
	driver.DriverContext
}

// Connector 接口表示mysql driver中表示连接器的对象上实现的接口集合
type Connector interface {
	driver.Connector
}

// Conn 接口表示mysql driver中表示连接的对象上实现的接口集合
type Conn interface {
	driver.Pinger
	driver.ExecerContext
	driver.QueryerContext
	driver.Conn
	driver.ConnPrepareContext
	driver.ConnBeginTx
	driver.SessionResetter
	driver.Validator
	driver.NamedValueChecker
	io.Closer
}

// Rows 接口表示mysql driver中表示行的对象上实现的接口集合
type Rows interface {
	driver.Rows
	driver.RowsNextResultSet
	driver.RowsColumnTypeScanType
	driver.RowsColumnTypeDatabaseTypeName
	driver.RowsColumnTypeNullable
	driver.RowsColumnTypePrecisionScale
	io.Closer
}

// Stmt 接口表示mysql driver中表示语句的对象上实现的接口集合
type Stmt interface {
	driver.Stmt
	driver.StmtExecContext
	driver.StmtQueryContext
	driver.NamedValueChecker
	driver.ColumnConverter
	io.Closer
}

// Tx 接口表示mysql driver中表示事务的对象上实现的接口集合
type Tx interface {
	driver.Tx
}

// Result 接口表示mysql driver中表示事务的对象上实现的接口集合
type Result interface {
	driver.Result
	// AllRowsAffected returns a slice containing the affected rows for each
	// executed statement.
	AllRowsAffected() []int64
	// AllLastInsertIds returns a slice containing the last inserted ID for each
	// executed statement.
	AllLastInsertIds() []int64
}

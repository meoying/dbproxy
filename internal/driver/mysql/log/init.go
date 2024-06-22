package log

import (
	"database/sql"

	"github.com/go-sql-driver/mysql"
)

func Open(name string) (*sql.DB, error) {
	driver := newDriver(&mysql.MySQLDriver{}, newDefaultLogger())
	connector, err := driver.OpenConnector(name)
	if err != nil {
		return nil, err
	}
	return sql.OpenDB(connector), nil
}

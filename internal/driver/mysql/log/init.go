package log

import (
	_ "github.com/go-sql-driver/mysql"
)

func init() {
	// TODO: github.com/go-sql-driver/mysql@v1.8.1/driver.go 中会注册MySQLDriver
	// 这里需要先将其装饰为log driver后再注册
	// 一种可行的方法是再go build时加上 "-ldflags=-X github.com/go-sql-driver/mysql.driverName=''"
	// 来关闭github.com/go-sql-driver/mysql 的init方法中注册

	// d := &mysql.MySQLDriver{}
	// logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	// sql.Register("mysql", newDriver(d, d, NewSLogger(logger)))
}

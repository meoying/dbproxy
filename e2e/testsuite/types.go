package testsuite

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ecodeclub/ekit/retry"
	"github.com/go-sql-driver/mysql"
	logdriver "github.com/meoying/dbproxy/internal/protocol/mysql/driver/log"
	"github.com/stretchr/testify/require"
)

const (
	// MYSQLDSNTmpl 直接连接MYSQL数据库时所用的DSN, 暂不支持?charset=utf8mb4&parseTime=True&loc=Local
	MYSQLDSNTmpl = "root:root@tcp(localhost:13306)/%s"
)

type Order struct {
	UserId  int
	OrderId int64
	Content string
	Amount  float64
}

type sqlInfo struct {
	query string
	args  []any
	// 执行 Exec 后返回的结果
	rowsAffected int64
	lastInsertId int64
}

func OpenDefaultDB() (*sql.DB, error) {
	return OpenSQLDB(fmt.Sprintf(MYSQLDSNTmpl, ""))
}

func CreateDatabases(t *testing.T, db *sql.DB, names ...string) {
	t.Helper()
	for _, name := range names {
		_, err := db.Exec(fmt.Sprintf("CREATE DATABASE IF NOT EXISTS `%s`", name))
		require.NoError(t, err, fmt.Errorf("创建库=%s失败", name))
	}
}

func CreateTables(t *testing.T, db *sql.DB, tableNames ...string) {
	t.Helper()
	const tableTemplate = "CREATE TABLE IF NOT EXISTS `%s` " +
		"(" +
		"user_id INT NOT NULL," +
		"order_id BIGINT NOT NULL," +
		"content TEXT," +
		"amount DOUBLE," +
		"PRIMARY KEY (user_id)" +
		") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;"
	if len(tableNames) == 0 {
		tableNames = append(tableNames, "order")
	}
	for _, name := range tableNames {
		c := fmt.Sprintf(tableTemplate, name)
		log.Printf("create table `%s`\n", c)
		_, err := db.Exec(c)
		require.NoError(t, err, fmt.Errorf("创建表=%s失败", name))
	}
}

// WaitForMySQLSetup 检查MySQL是否启动并返回一个可用的*sql.DB对象
func WaitForMySQLSetup(dsn string) *sql.DB {
	sqlDB, err := OpenSQLDB(dsn)
	if err != nil {
		panic(err)
	}
	const maxInterval = 10 * time.Second
	const maxRetries = 10
	strategy, err := retry.NewExponentialBackoffRetryStrategy(time.Second, maxInterval, maxRetries)
	if err != nil {
		panic(err)
	}
	const timeout = 5 * time.Second
	for {
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		err = sqlDB.PingContext(ctx)
		cancel()
		if err == nil {
			break
		}
		next, ok := strategy.Next()
		if !ok {
			panic("WaitForMySQLSetup 重试失败......")
		}
		time.Sleep(next)
	}
	return sqlDB
}

// OpenSQLDB 根据dsn创建一个*sql.DB对象其内部使用了logDriver
func OpenSQLDB(dsn string) (*sql.DB, error) {
	l := slog.New(slog.NewTextHandler(os.Stdout, nil))
	connector, err := logdriver.NewConnector(&mysql.MySQLDriver{}, dsn, logdriver.WithLogger(l))
	if err != nil {
		return nil, err
	}
	return sql.OpenDB(connector), nil
}

func execSQL(t *testing.T, db *sql.DB, sqls []string) {
	t.Helper()
	for _, vsql := range sqls {
		_, err := db.Exec(vsql)
		require.NoError(t, err)
	}
}

func getRowsFromTable(t *testing.T, db *sql.DB, ids []int64) *sql.Rows {
	t.Helper()
	idStr := make([]string, 0, len(ids))
	for _, id := range ids {
		idStr = append(idStr, strconv.FormatInt(id, 10))
	}
	query := fmt.Sprintf("SELECT /* @proxy useMaster=true; */ `user_id`, `order_id`, `content`, `amount` FROM `order` WHERE `user_id` in (%s)", strings.Join(idStr, ","))
	rows, err := db.Query(query)
	require.NoError(t, err)
	return rows
}

func getOrdersFromRows(t *testing.T, rows *sql.Rows) []Order {
	t.Helper()
	res := make([]Order, 0, 2)
	for rows.Next() {
		order := Order{}
		err := rows.Scan(&order.UserId, &order.OrderId, &order.Content, &order.Amount)
		require.NoError(t, err)
		res = append(res, order)
	}
	require.NoError(t, rows.Close())
	return res
}

func ClearTables(t *testing.T, db *sql.DB, tableNames ...string) {
	t.Helper()
	if len(tableNames) == 0 {
		tableNames = append(tableNames, "order")
	}
	for _, name := range tableNames {
		_, err := db.Exec(fmt.Sprintf("DELETE FROM `%s`;", name))
		require.NoError(t, err)
	}
}

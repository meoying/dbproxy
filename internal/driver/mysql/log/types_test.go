package log

import (
	"bytes"
	"database/sql"
	"fmt"
	"log"
	"log/slog"
	"os"
	"testing"

	"github.com/go-sql-driver/mysql"
	"github.com/meoying/dbproxy/internal/driver/mysql/mocks"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/mock/gomock"
)

func TestNewConnector(t *testing.T) {
	t.Run("默认log", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		d := mocks.NewMockDriver(ctrl)
		dsn := "valid_dsn"
		d.EXPECT().OpenConnector(dsn).Return(mocks.NewMockConnector(ctrl), nil)

		conn, err := NewConnector(d, dsn)

		require.NoError(t, err)
		assert.NotZero(t, conn)
	})
	t.Run("自定义log", func(t *testing.T) {
		ctrl := gomock.NewController(t)
		defer ctrl.Finish()

		d := mocks.NewMockDriver(ctrl)
		dsn := "valid_dsn"
		d.EXPECT().OpenConnector(dsn).Return(mocks.NewMockConnector(ctrl), nil)

		customLogger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
		conn, err := NewConnector(d, dsn, WithLogger(customLogger))

		require.NoError(t, err)
		assert.NotZero(t, conn)
	})
}

func ExampleNewConnector() {
	dsn := "root:root@tcp(example.com:3306)/?charset=utf8mb4&parseTime=True&loc=Local"
	driver := &mysql.MySQLDriver{}

	customLogger := slog.New(slog.NewJSONHandler(&bytes.Buffer{}, nil))
	connector, err := NewConnector(driver, dsn, WithLogger(customLogger))
	if err != nil {
		log.Fatalf("创建连接器失败: %v", err)
	}
	db := sql.OpenDB(connector)
	fmt.Println("使用自定义log创建连接器并获取*sql.DB对象:", db != nil)

	// Output:
	// 使用自定义log创建连接器并获取*sql.DB对象: true
}

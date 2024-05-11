package cmd

import (
	"database/sql"
	"testing"

	"gitee.com/meoying/dbproxy/internal/protocol/mysql/internal/flags"
	"gitee.com/meoying/dbproxy/internal/protocol/mysql/internal/query"
	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestQueryExecutor_parseQuery(t *testing.T) {
	// 这些测试用例的 payload 来自真实的数据库查询
	testCases := []struct {
		name string

		payload []byte

		wantQue query.Query
	}{
		{
			name: "无参数",
			// 要忽略前面的 4 个字节，是头部
			payload: []byte{7, 0, 0, 2,
				3, 83, 69, 76, 69, 67, 84, 32,
				42, 32, 70, 82, 79, 77, 32, 96,
				117, 115, 101, 114, 115, 96}[4:],
			wantQue: query.Query{
				SQL: "SELECT * FROM `users`",
			},
		},
		{
			// 实际上这种场景在 Go driver 发请求的时候，当成了没有参数来处理
			name: "参数直接在SQL中",
			// 要忽略前面的 4 个字节，是头部
			payload: []byte{7, 0, 0, 2, 3, 83, 69, 76, 69, 67, 84, 32, 42, 32, 70, 82, 79, 77, 32, 96, 117, 115, 101, 114, 115, 96, 32, 87, 72, 69, 82, 69, 32, 105, 100, 61, 49}[4:],
			wantQue: query.Query{
				SQL: "SELECT * FROM `users` WHERE id=1",
			},
		},
		{
			name: "interpolateParams=true",
			// payload 对应的查询是
			// db.Query("SELECT * FROM `users` WHERE id=?", 1)
			// 并且 dsn 设置了 interpolateParams=true
			payload: []byte{83, 69, 76, 69, 3, 83, 69, 76, 69, 67, 84, 32, 42, 32, 70, 82, 79, 77, 32, 96, 117, 115, 101, 114, 115, 96, 32, 87, 72, 69, 82, 69, 32, 105, 100, 61, 49}[4:],
			wantQue: query.Query{
				SQL: "SELECT * FROM `users` WHERE id=1",
			},
		},
	}
	exec := &QueryExecutor{}
	ctx := &Context{
		// 只设置一个
		CapabilityFlags: flags.CapabilityFlags(flags.ClientQueryAttributes),
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			que := exec.parseQuery(ctx, tc.payload)
			assert.Equal(t, tc.wantQue, que)
		})
	}
}

// TestGeneratePayload 我用来辅助生成 payload
func TestGeneratePayload(t *testing.T) {
	// 必须加上 interpolateParams=true
	const dsn = "root:root@tcp(local.ubuntu:13316)/webook?interpolateParams=true"
	db, err := sql.Open("mysql", dsn)
	require.NoError(t, err)
	_, err = db.Query("SELECT * FROM `users` WHERE id=?", 1)
	require.NoError(t, err)
}

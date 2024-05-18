package rwsplit

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestMySQLHandler_usingMaster(t *testing.T) {
	testCases := []struct {
		name  string
		query string

		wantRes bool
	}{
		{
			name:    "普通SELECT语句",
			query:   "SELECT * FROM users",
			wantRes: false,
		},
		{
			name:    "前导空格",
			query:   "   		SELECT * FROM users",
			wantRes: false,
		},
		{
			name:    "带注释",
			query:   "SELECT /*! @USE_MASTER true */ * FROM users",
			wantRes: true,
		},
		{
			name:    "字符串中含有 select ",
			query:   "SELECT * FROM users WHERE name='select';",
			wantRes: false,
		},
		{
			name:    "普通的UPDATE语句",
			query:   "UPDATE users set `name`='Tom'",
			wantRes: true,
		},
		{
			name:  "误判-INSERT-SELECT",
			query: "INSERT INTO users_copy SELECT * FROM users",
			// 预期应该走主库的，但是这个会被误判为走从库
			wantRes: false,
		},
	}

	hdl := &MySQLHandler{}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			ok := hdl.usingMaster(tc.query)
			assert.Equal(t, tc.wantRes, ok)
		})
	}
}

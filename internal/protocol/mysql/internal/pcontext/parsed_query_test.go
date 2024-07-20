package pcontext_test

import (
	"testing"

	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/pcontext"
	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/visitor/vparser"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParsedQuery(t *testing.T) {
	testcases := []struct {
		name  string
		query string

		wantHints []string
		wantType  string
	}{
		{
			name:      "查询语句",
			query:     "SELECT /*useMaster*/ * FROM mytable",
			wantHints: []string{"/*useMaster*/"},
			wantType:  vparser.SelectStmt,
		},
		{
			name:     "插入语句",
			query:    "INSERT INTO mytable VALUES (1)",
			wantType: vparser.InsertStmt,
		},
	}
	for _, tc := range testcases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			p := pcontext.NewParsedQuery(tc.query)
			require.NotNil(t, p.Root())
			assert.Equal(t, tc.wantType, p.Type())
			assert.Equal(t, tc.wantHints, p.Hints())
		})
	}
}

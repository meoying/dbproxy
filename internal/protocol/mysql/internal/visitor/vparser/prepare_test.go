package vparser

import (
	"testing"

	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/ast"
	"github.com/stretchr/testify/assert"
)

func TestPrepareVisitor_Parse(t *testing.T) {
	testcases := []struct {
		name    string
		sql     string
		wantVal PrepareVal
		err     error
	}{
		{
			name: "一个占位符",
			sql:  "INSERT INTO users (id) VALUE (?);",
			wantVal: PrepareVal{
				PlaceHolderCount: 1,
			},
		},
		{
			name: "多个占位符",
			sql:  "INSERT INTO users (id,id2) VALUES (?,?);",
			wantVal: PrepareVal{
				PlaceHolderCount: 2,
			},
		},
		{
			name: "多参数，1个占位符",
			sql:  "INSERT INTO users (id,id2) VALUES (1,?);",
			wantVal: PrepareVal{
				PlaceHolderCount: 1,
			},
		},
	}
	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			root := ast.Parse(tc.sql)
			prepareVisitor := NewPrepareVisitor()
			resp := prepareVisitor.Parse(root)
			actualBaseVal := resp.(BaseVal)
			assert.Equal(t, tc.err, actualBaseVal.Err)
			if actualBaseVal.Err != nil {
				return
			}
			pVal := actualBaseVal.Data.(PrepareVal)
			assert.Equal(t, tc.wantVal, pVal)
		})
	}
}

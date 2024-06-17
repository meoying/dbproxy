package vparser

import (
	"testing"

	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/ast"
	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/ast/parser"
	"github.com/stretchr/testify/assert"
)

func TestVisitor_Insert(t *testing.T) {
	testcases := []struct {
		name    string
		sql     string
		wantVal InsertVal
		err     error
	}{
		{
			name: "插入一条数据",
			sql:  "INSERT INTO users (id,id2,id3,id4,str,has,no) VALUE (0,1,11,1.1,\"z\",true,null);",
			wantVal: InsertVal{
				Cols: []string{
					"id",
					"id2",
					"id3",
					"id4",
					"str",
					"has",
					"no",
				},
				TableName: "users",
				Vals: []ValMap{
					{
						"id":  0,
						"id2": 1,
						"id3": 11,
						"id4": 1.1,
						"str": "z",
						"has": true,
						"no":  nil,
					},
				},
			},
		},
		{
			name: "插入多条数据",
			sql:  "INSERT INTO users (id,id2,id3,id4,str,has,no) VALUES (0,1,11,1.1,\"z\",true,null),(1,2,3,5.1,'zm',false,10);",
			wantVal: InsertVal{
				Cols: []string{
					"id",
					"id2",
					"id3",
					"id4",
					"str",
					"has",
					"no",
				},
				TableName: "users",
				Vals: []ValMap{
					{
						"id":  0,
						"id2": 1,
						"id3": 11,
						"id4": 1.1,
						"str": "z",
						"has": true,
						"no":  nil,
					},
					{
						"id":  1,
						"id2": 2,
						"id3": 3,
						"id4": 5.1,
						"str": "zm",
						"has": false,
						"no":  10,
					},
				},
			},
		},
	}
	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			root := ast.Parse(tc.sql)
			baseVisitor := &BaseVisitor{}
			q := InsertVisitor{
				BaseVisitor: baseVisitor,
			}
			actualVal := q.VisitRoot(root.(*parser.RootContext))
			actualBaseVal := actualVal.(BaseVal)
			assert.Equal(t, tc.err, actualBaseVal.Err)
			if actualBaseVal.Err != nil {
				return
			}
			insertVal := actualBaseVal.Data.(InsertVal)
			insertVal.AstValues = nil
			assert.Equal(t, tc.wantVal, insertVal)
		})
	}

}

package visitor

import (
	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/ast"
	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/ast/parser"
	"github.com/stretchr/testify/assert"
	"reflect"
	"testing"
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
						"id": {
							Val: 0,
							Typ: reflect.Int,
						},
						"id2": {
							Val: 1,
							Typ: reflect.Int,
						},
						"id3": {
							Val: 11,
							Typ: reflect.Int,
						},
						"id4": {
							Val: float64(1.1),
							Typ: reflect.Float64,
						},
						"str": {
							Val: "z",
							Typ: reflect.String,
						},
						"has": {
							Val: true,
							Typ: reflect.Bool,
						},
						"no": {
							Val: nil,
							Typ: reflect.Invalid,
						},
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
						"id": {
							Val: 0,
							Typ: reflect.Int,
						},
						"id2": {
							Val: 1,
							Typ: reflect.Int,
						},
						"id3": {
							Val: 11,
							Typ: reflect.Int,
						},
						"id4": {
							Val: float64(1.1),
							Typ: reflect.Float64,
						},
						"str": {
							Val: "z",
							Typ: reflect.String,
						},
						"has": {
							Val: true,
							Typ: reflect.Bool,
						},
						"no": {
							Val: nil,
							Typ: reflect.Invalid,
						},
					},
					{
						"id": {
							Val: 1,
							Typ: reflect.Int,
						},
						"id2": {
							Val: 2,
							Typ: reflect.Int,
						},
						"id3": {
							Val: 3,
							Typ: reflect.Int,
						},
						"id4": {
							Val: float64(5.1),
							Typ: reflect.Float64,
						},
						"str": {
							Val: "zm",
							Typ: reflect.String,
						},
						"has": {
							Val: false,
							Typ: reflect.Bool,
						},
						"no": {
							Val: 10,
							Typ: reflect.Int,
						},
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
			assert.Equal(t, tc.wantVal, actualBaseVal.Data.(InsertVal))
		})
	}

}

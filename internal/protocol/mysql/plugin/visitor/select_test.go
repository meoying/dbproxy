package visitor

import (
	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/ast"
	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/ast/parser"
	"github.com/meoying/dbproxy/internal/sharding/operator"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestSelectVisitor(t *testing.T) {
	testcases := []struct {
		name    string
		sql     string
		wantVal SelectVal
		wantErr error
	}{
		{
			name: "单个比较符",
			sql:  "select id from t1 where id > 11",
			wantVal: SelectVal{
				Cols: []Column{
					{
						Name: "id",
					},
				},
				Predicate: Predicate{
					Left: Column{
						Name: "id",
					},
					Op:    operator.OpGT,
					Right: ValueOf(11),
				},
			},
		},
		{
			name: "单个比较符 >=",
			sql:  "select id from t1 where id >= 11",
			wantVal: SelectVal{
				Cols: []Column{
					{
						Name: "id",
					},
				},
				Predicate: Predicate{
					Left: Column{
						Name: "id",
					},
					Op:    operator.OpGTEQ,
					Right: ValueOf(11),
				},
			},
		},
		{
			name: "单个比较符，一侧为计算表达式",
			sql:  "select id from t1 where id + 1 >= 11",
			wantVal: SelectVal{
				Cols: []Column{
					{
						Name: "id",
					},
				},
				Predicate: Predicate{
					Left: Predicate{
						Left: Column{
							Name: "id",
						},
						Op:    operator.OpAdd,
						Right: ValueOf(1),
					},
					Op:    operator.OpGTEQ,
					Right: ValueOf(11),
				},
			},
		},
		{
			name: "单个比较符，一侧为二元计算表达式",
			sql:  "select id from t1 where id +1 + 1 >= 11",
			wantVal: SelectVal{
				Cols: []Column{
					{
						Name: "id",
					},
				},
				Predicate: Predicate{
					Left: Predicate{
						Left: Predicate{
							Left: Column{
								Name: "id",
							},
							Op:    operator.OpAdd,
							Right: ValueOf(1),
						},
						Op:    operator.OpAdd,
						Right: ValueOf(1),
					},
					Op:    operator.OpGTEQ,
					Right: ValueOf(11),
				},
			},
		},
		{
			name: "单个比较符，两侧为一元计算表达式",
			sql:  "select id from t1 where id +1 >= a * 1",
			wantVal: SelectVal{
				Cols: []Column{
					{
						Name: "id",
					},
				},
				Predicate: Predicate{
					Left: Predicate{
						Left: Column{
							Name: "id",
						},
						Op:    operator.OpAdd,
						Right: ValueOf(1),
					},
					Op: operator.OpGTEQ,
					Right: Predicate{
						Left: Column{
							Name: "a",
						},
						Op:    operator.OpMulti,
						Right: ValueOf(1),
					},
				},
			},
		},
		{
			name: "like查询",
			sql:  "select id from t1 where name like '%n%';",
			wantVal: SelectVal{
				Cols: []Column{
					{
						Name: "id",
					},
				},
				Predicate: Predicate{
					Left: Column{
						Name: "name",
					},
					Op:    operator.OpLike,
					Right: ValueOf("%n%"),
				},
			},
		},
		{
			name: "in 查询",
			sql:  "select id from t1 where id in (1,2,3,4);",
			wantVal: SelectVal{
				Cols: []Column{
					{
						Name: "id",
					},
				},
				Predicate: Predicate{
					Left: Column{
						Name: "id",
					},
					Op: operator.OpIn,
					Right: Values{
						Vals: []any{1, 2, 3, 4},
					},
				},
			},
		},
		{
			name: "Not in查询",
			sql:  "select id from t1 where id not in (1,2,3,4);",
			wantVal: SelectVal{
				Cols: []Column{
					{
						Name: "id",
					},
				},
				Predicate: Predicate{
					Left: Column{
						Name: "id",
					},
					Op: operator.OpNotIN,
					Right: Values{
						Vals: []any{1, 2, 3, 4},
					},
				},
			},
		},
		{
			name: "and 查询",
			sql:  "select id from t1 where a > 10 and b < 10;",
			wantVal: SelectVal{
				Cols: []Column{
					{
						Name: "id",
					},
				},
				Predicate: Predicate{
					Left: Predicate{
						Left: Column{
							Name: "a",
						},
						Op:    operator.OpGT,
						Right: ValueOf(10),
					},
					Op: operator.OpAnd,
					Right: Predicate{
						Left: Column{
							Name: "b",
						},
						Op:    operator.OpLT,
						Right: ValueOf(10),
					},
				},
			},
		},
		{
			name: "有多个逻辑运算符",
			sql:  "select id from t1 where (a > 10 or b <=10)  and b like '%name%';",
			wantVal: SelectVal{
				Cols: []Column{
					{
						Name: "id",
					},
				},
				Predicate: Predicate{
					Left: Predicate{
						Left: Predicate{
							Left: Column{
								Name: "a",
							},
							Op:    operator.OpGT,
							Right: ValueOf(10),
						},
						Op: operator.OpOr,
						Right: Predicate{
							Left: Column{
								Name: "b",
							},
							Op:    operator.OpLTEQ,
							Right: ValueOf(10),
						},
					},
					Op: operator.OpAnd,
					Right: Predicate{
						Left: Column{
							Name: "b",
						},
						Op:    operator.OpLike,
						Right: ValueOf("%name%"),
					},
				},
			},
		},
		{
			name: "有多个逻辑运算符",
			sql:  "select id from t1 where (a > 10 or b <=10)  and (b like '%name%');",
			wantVal: SelectVal{
				Cols: []Column{
					{
						Name: "id",
					},
				},
				Predicate: Predicate{
					Left: Predicate{
						Left: Predicate{
							Left: Column{
								Name: "a",
							},
							Op:    operator.OpGT,
							Right: ValueOf(10),
						},
						Op: operator.OpOr,
						Right: Predicate{
							Left: Column{
								Name: "b",
							},
							Op:    operator.OpLTEQ,
							Right: ValueOf(10),
						},
					},
					Op: operator.OpAnd,
					Right: Predicate{
						Left: Column{
							Name: "b",
						},
						Op:    operator.OpLike,
						Right: ValueOf("%name%"),
					},
				},
			},
		},
		{
			name: "括号里套括号",
			sql:  "select id from t1 where (a > 10 or (b <=10 and c > 0 ))  and (b like '%name%');",
			wantVal: SelectVal{
				Cols: []Column{
					{
						Name: "id",
					},
				},
				Predicate: Predicate{
					Left: Predicate{
						Left: Predicate{
							Left: Column{
								Name: "a",
							},
							Op:    operator.OpGT,
							Right: ValueOf(10),
						},
						Op: operator.OpOr,
						Right: Predicate{
							Left: Predicate{
								Left: Column{
									Name: "b",
								},
								Op:    operator.OpLTEQ,
								Right: ValueOf(10),
							},
							Op: operator.OpAnd,
							Right: Predicate{
								Left: Column{
									Name: "c",
								},
								Op:    operator.OpGT,
								Right: ValueOf(0),
							},
						},
					},
					Op: operator.OpAnd,
					Right: Predicate{
						Left: Column{
							Name: "b",
						},
						Op:    operator.OpLike,
						Right: ValueOf("%name%"),
					},
				},
			},
		},
		{
			name: "not 查询",
			sql:  "select id from t1 where not (id > 10 and c < 19);",
			wantVal: SelectVal{
				Cols: []Column{
					{
						Name: "id",
					},
				},
				Predicate: Predicate{
					Left: Raw(""),
					Op:   operator.OpNot,
					Right: Predicate{
						Left: Predicate{
							Left: Column{
								Name: "id",
							},
							Op:    operator.OpGT,
							Right: ValueOf(10),
						},
						Op: operator.OpAnd,
						Right: Predicate{
							Left: Column{
								Name: "c",
							},
							Op:    operator.OpLT,
							Right: ValueOf(19),
						},
					},
				},
			},
		},
		{
			name: "where 的变量名含有 `` ",
			sql:  "select id from t1 where `id` > 11",
			wantVal: SelectVal{
				Cols: []Column{
					{
						Name: "id",
					},
				},
				Predicate: Predicate{
					Left: Column{
						Name: "id",
					},
					Op:    operator.OpGT,
					Right: ValueOf(11),
				},
			},
		},
		{
			name: "select 列中包含`",
			sql:  "select `id`,`name` from  t1;",
			wantVal: SelectVal{
				Cols: []Column{
					{
						Name: "id",
					},
					{
						Name: "name",
					},
				},
			},
		},
		{
			name: "select 列为 *",
			sql:  "select * from t1;",
			wantVal: SelectVal{
				Cols: []Column{},
			},
		},
		{
			name: "select 列中有别名",
			sql:  "select id as a from t1;",
			wantVal: SelectVal{
				Cols: []Column{
					{
						Name:  "id",
						Alias: "a",
					},
				},
			},
		},

	}
	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			root := ast.Parse(tc.sql)
			selectV := &SelectVisitor{
				BaseVisitor: &BaseVisitor{},
			}
			resp := selectV.VisitRoot(root.(*parser.RootContext))
			res := resp.(BaseVal)
			assert.Equal(t, tc.wantErr, res.Err)
			if res.Err != nil {
				return
			}
			actual := res.Data.(SelectVal)
			assert.Equal(t, tc.wantVal, actual)

		})
	}
}

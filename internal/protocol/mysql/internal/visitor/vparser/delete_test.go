package vparser

import (
	"testing"

	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/ast"
	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/visitor"
	"github.com/meoying/dbproxy/internal/sharding/operator"
	"github.com/stretchr/testify/assert"
)

func TestDeleteVisitor(t *testing.T) {
	testcases := []struct {
		name    string
		sql     string
		wantVal DeleteVal
		wantErr error
	}{
		{
			name: "单个比较符",
			sql:  "delete from t1 where id > 11",
			wantVal: DeleteVal{

				Predicate: visitor.Predicate{
					Left: visitor.Column{
						Name: "id",
					},
					Op:    operator.OpGT,
					Right: visitor.ValueOf(11),
				},
			},
		},
		{
			name: "单个比较符 >=",
			sql:  "delete from t1 where id >= 11",
			wantVal: DeleteVal{
				Predicate: visitor.Predicate{
					Left: visitor.Column{
						Name: "id",
					},
					Op:    operator.OpGTEQ,
					Right: visitor.ValueOf(11),
				},
			},
		},
		{
			name: "单个比较符，一侧为计算表达式",
			sql:  "delete from t1 where id + 1 >= 11",
			wantVal: DeleteVal{
				Predicate: visitor.Predicate{
					Left: visitor.Predicate{
						Left: visitor.Column{
							Name: "id",
						},
						Op:    operator.OpAdd,
						Right: visitor.ValueOf(1),
					},
					Op:    operator.OpGTEQ,
					Right: visitor.ValueOf(11),
				},
			},
		},
		{
			name: "单个比较符，一侧为二元计算表达式",
			sql:  "delete from t1 where id +1 + 1 >= 11",
			wantVal: DeleteVal{
				Predicate: visitor.Predicate{
					Left: visitor.Predicate{
						Left: visitor.Predicate{
							Left: visitor.Column{
								Name: "id",
							},
							Op:    operator.OpAdd,
							Right: visitor.ValueOf(1),
						},
						Op:    operator.OpAdd,
						Right: visitor.ValueOf(1),
					},
					Op:    operator.OpGTEQ,
					Right: visitor.ValueOf(11),
				},
			},
		},
		{
			name: "单个比较符，两侧为一元计算表达式",
			sql:  "delete from t1 where id +1 >= a * 1",
			wantVal: DeleteVal{
				Predicate: visitor.Predicate{
					Left: visitor.Predicate{
						Left: visitor.Column{
							Name: "id",
						},
						Op:    operator.OpAdd,
						Right: visitor.ValueOf(1),
					},
					Op: operator.OpGTEQ,
					Right: visitor.Predicate{
						Left: visitor.Column{
							Name: "a",
						},
						Op:    operator.OpMulti,
						Right: visitor.ValueOf(1),
					},
				},
			},
		},
		{
			name: "like查询",
			sql:  "delete from t1 where name like '%n%';",
			wantVal: DeleteVal{

				Predicate: visitor.Predicate{
					Left: visitor.Column{
						Name: "name",
					},
					Op:    operator.OpLike,
					Right: visitor.ValueOf("%n%"),
				},
			},
		},
		{
			name: "in 查询",
			sql:  "delete from t1 where id in (1,2,3,4);",
			wantVal: DeleteVal{
				Predicate: visitor.Predicate{
					Left: visitor.Column{
						Name: "id",
					},
					Op: operator.OpIn,
					Right: visitor.Values{
						Vals: []any{1, 2, 3, 4},
					},
				},
			},
		},
		{
			name: "Not in查询",
			sql:  "delete from t1 where id not in (1,2,3,4);",
			wantVal: DeleteVal{
				Predicate: visitor.Predicate{
					Left: visitor.Column{
						Name: "id",
					},
					Op: operator.OpNotIN,
					Right: visitor.Values{
						Vals: []any{1, 2, 3, 4},
					},
				},
			},
		},
		{
			name: "and 查询",
			sql:  "delete from t1 where a > 10 and b < 10;",
			wantVal: DeleteVal{
				Predicate: visitor.Predicate{
					Left: visitor.Predicate{
						Left: visitor.Column{
							Name: "a",
						},
						Op:    operator.OpGT,
						Right: visitor.ValueOf(10),
					},
					Op: operator.OpAnd,
					Right: visitor.Predicate{
						Left: visitor.Column{
							Name: "b",
						},
						Op:    operator.OpLT,
						Right: visitor.ValueOf(10),
					},
				},
			},
		},
		{
			name: "有多个逻辑运算符",
			sql:  "delete from t1 where (a > 10 or b <=10)  and b like '%name%';",
			wantVal: DeleteVal{
				Predicate: visitor.Predicate{
					Left: visitor.Predicate{
						Left: visitor.Predicate{
							Left: visitor.Column{
								Name: "a",
							},
							Op:    operator.OpGT,
							Right: visitor.ValueOf(10),
						},
						Op: operator.OpOr,
						Right: visitor.Predicate{
							Left: visitor.Column{
								Name: "b",
							},
							Op:    operator.OpLTEQ,
							Right: visitor.ValueOf(10),
						},
					},
					Op: operator.OpAnd,
					Right: visitor.Predicate{
						Left: visitor.Column{
							Name: "b",
						},
						Op:    operator.OpLike,
						Right: visitor.ValueOf("%name%"),
					},
				},
			},
		},
		{
			name: "有多个逻辑运算符",
			sql:  "delete from t1 where (a > 10 or b <=10)  and (b like '%name%');",
			wantVal: DeleteVal{
				Predicate: visitor.Predicate{
					Left: visitor.Predicate{
						Left: visitor.Predicate{
							Left: visitor.Column{
								Name: "a",
							},
							Op:    operator.OpGT,
							Right: visitor.ValueOf(10),
						},
						Op: operator.OpOr,
						Right: visitor.Predicate{
							Left: visitor.Column{
								Name: "b",
							},
							Op:    operator.OpLTEQ,
							Right: visitor.ValueOf(10),
						},
					},
					Op: operator.OpAnd,
					Right: visitor.Predicate{
						Left: visitor.Column{
							Name: "b",
						},
						Op:    operator.OpLike,
						Right: visitor.ValueOf("%name%"),
					},
				},
			},
		},
		{
			name: "括号里套括号",
			sql:  "delete from t1 where (a > 10 or (b <=10 and c > 0 ))  and (b like '%name%');",
			wantVal: DeleteVal{
				Predicate: visitor.Predicate{
					Left: visitor.Predicate{
						Left: visitor.Predicate{
							Left: visitor.Column{
								Name: "a",
							},
							Op:    operator.OpGT,
							Right: visitor.ValueOf(10),
						},
						Op: operator.OpOr,
						Right: visitor.Predicate{
							Left: visitor.Predicate{
								Left: visitor.Column{
									Name: "b",
								},
								Op:    operator.OpLTEQ,
								Right: visitor.ValueOf(10),
							},
							Op: operator.OpAnd,
							Right: visitor.Predicate{
								Left: visitor.Column{
									Name: "c",
								},
								Op:    operator.OpGT,
								Right: visitor.ValueOf(0),
							},
						},
					},
					Op: operator.OpAnd,
					Right: visitor.Predicate{
						Left: visitor.Column{
							Name: "b",
						},
						Op:    operator.OpLike,
						Right: visitor.ValueOf("%name%"),
					},
				},
			},
		},
		{
			name: "not 查询",
			sql:  "delete from t1 where not (id > 10 and c < 19);",
			wantVal: DeleteVal{
				Predicate: visitor.Predicate{
					Left: visitor.Raw(""),
					Op:   operator.OpNot,
					Right: visitor.Predicate{
						Left: visitor.Predicate{
							Left: visitor.Column{
								Name: "id",
							},
							Op:    operator.OpGT,
							Right: visitor.ValueOf(10),
						},
						Op: operator.OpAnd,
						Right: visitor.Predicate{
							Left: visitor.Column{
								Name: "c",
							},
							Op:    operator.OpLT,
							Right: visitor.ValueOf(19),
						},
					},
				},
			},
		},
		{
			name: "where 的变量名含有 `` ",
			sql:  "delete from t1 where `id` > 11",
			wantVal: DeleteVal{
				Predicate: visitor.Predicate{
					Left: visitor.Column{
						Name: "id",
					},
					Op:    operator.OpGT,
					Right: visitor.ValueOf(11),
				},
			},
		},
		{
			name:    "没有where条件",
			sql:     "delete from t1;",
			wantVal: DeleteVal{},
		},
	}
	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			root := ast.Parse(tc.sql)
			deleteVisitor := NewDeleteVisitor()
			resp := deleteVisitor.Parse(root)
			res := resp.(BaseVal)
			assert.Equal(t, tc.wantErr, res.Err)
			if res.Err != nil {
				return
			}
			actual := res.Data.(DeleteVal)
			assert.Equal(t, tc.wantVal, actual)
		})
	}
}

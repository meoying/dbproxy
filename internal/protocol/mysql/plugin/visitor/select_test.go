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
				Cols: []Selectable{
					Column{
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
				Cols: []Selectable{
					Column{
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
				Cols: []Selectable{
					Column{
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
				Cols: []Selectable{
					Column{
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
				Cols: []Selectable{
					Column{
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
				Cols: []Selectable{
					Column{
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
				Cols: []Selectable{
					Column{
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
				Cols: []Selectable{
					Column{
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
				Cols: []Selectable{
					Column{
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
				Cols: []Selectable{
					Column{
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
				Cols: []Selectable{
					Column{
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
				Cols: []Selectable{
					Column{
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
				Cols: []Selectable{
					Column{
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
				Cols: []Selectable{
					Column{
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
				Cols: []Selectable{
					Column{
						Name: "id",
					},
					Column{
						Name: "name",
					},
				},
			},
		},
		{
			name: "select 列为 *",
			sql:  "select * from t1;",
			wantVal: SelectVal{
				Cols: []Selectable{},
			},
		},
		{
			name: "select 列中有别名",
			sql:  "select id as a from t1;",
			wantVal: SelectVal{
				Cols: []Selectable{
					Column{
						Name:  "id",
						Alias: "a",
					},
				},
			},
		},
		{
			name: "聚合函数 Avg(id)",
			sql:  "select Avg(id)  from t1;",
			wantVal: SelectVal{
				Cols: []Selectable{
					Avg("id"),
				},
			},
		},
		{
			name: "聚合函数 Sum(id)",
			sql:  "select SUM(id)  from t1;",
			wantVal: SelectVal{
				Cols: []Selectable{
					Sum("id"),
				},
			},
		},
		{
			name: "聚合函数 Max(id)",
			sql:  "select MAX(id)  from t1;",
			wantVal: SelectVal{
				Cols: []Selectable{
					Max("id"),
				},
			},
		},
		{
			name: "聚合函数 Min(id)",
			sql:  "select MIN(id)  from t1;",
			wantVal: SelectVal{
				Cols: []Selectable{
					Min("id"),
				},
			},
		},
		{
			name: "聚合函数Count(*)",
			sql:  "select COUNT(*) from t1;",
			wantVal: SelectVal{
				Cols: []Selectable{
					Count("*"),
				},
			},
		},
		{
			name: "聚合函数字段带有`",
			sql:  "select COUNT(`id`) from t1;",
			wantVal: SelectVal{
				Cols: []Selectable{
					Count("id"),
				},
			},
		},
		{
			name: "聚合函数 Avg(id) as",
			sql:  "select Avg(id) as avgId  from t1;",
			wantVal: SelectVal{
				Cols: []Selectable{
					Avg("id").As("avgId"),
				},
			},
		},
		{
			name: "Distinct",
			sql:  "select Distinct id as avgId  from t1;",
			wantVal: SelectVal{
				Distinct: true,
				Cols: []Selectable{
					Column{
						Name:  "id",
						Alias: "avgId",
					},
				},
			},
		},
		{
			name: "Avg(Distinct id)",
			sql:  "select avg(Distinct id)as avgId  from t1;",
			wantVal: SelectVal{
				Cols: []Selectable{
					AvgDistinct("id").As("avgId"),
				},
			},
		},
		{
			name: "聚合函数count(*)",
			sql:  "select count(*) from t1;",
			wantVal: SelectVal{
				Cols: []Selectable{
					Count("*"),
				},
			},
		},
		{
			name: "聚合函数count(1)",
			sql:  "select count(1) from t1",
			wantVal: SelectVal{
				Cols: []Selectable{
					Count("1"),
				},
			},
		},
		{
			name: "聚合函数count(`id`)",
			sql:  "select count(`id`) from t1",
			wantVal: SelectVal{
				Cols: []Selectable{
					Count("`id`"),
				},
			},
		},
		{
			name: "单个order by",
			sql:  "select * from t1 order by id;",
			wantVal: SelectVal{
				Cols: []Selectable{},
				OrderClauses: []OrderClause{
					{
						Column: Column{
							Name: "id",
						},
					},
				},
			},
		},
		{
			name: "多个order by",
			sql:  "select * from t1 order by col1 asc,col2 desc",
			wantVal: SelectVal{
				Cols: []Selectable{},
				OrderClauses: []OrderClause{
					{
						Column: Column{
							Name: "col1",
						},
					},
					{
						Column: Column{
							Name: "col2",
						},
						Desc: true,
					},
				},
			},
		},
		{
			name: "多个group by",
			sql:  "select * from t1 group by col1,col2",
			wantVal: SelectVal{
				Cols: []Selectable{},
				GroupByClause: []Column{
					{
						Name: "col1",
					},
					{
						Name: "col2",
					},
				},
			},
		},
		{
			name: "limit",
			sql:  "select * from t1  limit 10 offset 1;",
			wantVal: SelectVal{
				Cols: []Selectable{},
				LimitClause: &LimitClause{
					Limit:  10,
					Offset: 1,
				},
			},
		},
		{
			name: "包含order by,group by,limit 子句",
			sql: `SELECT 
    product,
    count(id)
FROM 
    sales
WHERE id > 10
GROUP BY 
    product
ORDER BY 
    total_sales_amount DESC
LIMIT 10;
`,
			wantVal: SelectVal{
				Cols: []Selectable{
					Column{
						Name: "product",
					},
					Count("id"),
				},
				Predicate: Predicate{
					Left: Column{
						Name: "id",
					},
					Op:    operator.OpGT,
					Right: ValueOf(10),
				},
				GroupByClause: []Column{
					{
						Name: "product",
					},
				},
				OrderClauses: []OrderClause{
					{
						Desc: true,
						Column: Column{
							Name: "total_sales_amount",
						},
					},
				},
				LimitClause: &LimitClause{
					Limit: 10,
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

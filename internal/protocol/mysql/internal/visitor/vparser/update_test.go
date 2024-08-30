package vparser

import (
	"testing"

	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/ast"
	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/visitor"
	"github.com/meoying/dbproxy/internal/sharding/operator"
	"github.com/stretchr/testify/assert"
)

func TestUpdateVisitor(t *testing.T) {
	testcases := []struct {
		name    string
		sql     string
		wantVal UpdateVal
		wantErr error
	}{
		{
			name: "多个set语句",
			sql:  "UPDATE employees SET `salary` = 75000, position = 'Senior Developer' WHERE employee_id = 101;",
			wantVal: UpdateVal{
				Assigns: []visitor.Assignable{
					visitor.Assignment{
						Left: visitor.Column{
							Name: "salary",
						},
						Op:    operator.OpEQ,
						Right: visitor.ValueOf(75000),
					},
					visitor.Assignment{
						Left: visitor.Column{
							Name: "position",
						},
						Op:    operator.OpEQ,
						Right: visitor.ValueOf("Senior Developer"),
					},
				},
				Predicate: visitor.Predicate{
					Left: visitor.Column{
						Name: "employee_id",
					},
					Op:    operator.OpEQ,
					Right: visitor.ValueOf(101),
				},
			},
		},
		{
			name: "单个set语句",
			sql:  "UPDATE employees SET salary = 75000 WHERE employee_id = 101;",
			wantVal: UpdateVal{
				Assigns: []visitor.Assignable{
					visitor.Assignment{
						Left: visitor.Column{
							Name: "salary",
						},
						Op:    operator.OpEQ,
						Right: visitor.ValueOf(75000),
					},
				},
				Predicate: visitor.Predicate{
					Left: visitor.Column{
						Name: "employee_id",
					},
					Op:    operator.OpEQ,
					Right: visitor.ValueOf(101),
				},
			},
		},
		{
			name: "set右边为算术表达式",
			sql:  "update t1 set a = a +1;",
			wantVal: UpdateVal{
				Assigns: []visitor.Assignable{
					visitor.Assignment{
						Left: visitor.Column{
							Name: "a",
						},
						Op: operator.OpEQ,
						Right: visitor.Predicate{
							Left: visitor.Column{
								Name: "a",
							},
							Op:    operator.OpAdd,
							Right: visitor.ValueOf(1),
						},
					},
				},
			},
		},
		{
			name: "set右边为复杂算术表达式",
			sql:  "update t1 set a = (b + (a + 1)) * 10;",
			wantVal: UpdateVal{
				Assigns: []visitor.Assignable{
					visitor.Assignment{
						Left: visitor.Column{
							Name: "a",
						},
						Op: operator.OpEQ,
						Right: visitor.Predicate{
							Left: visitor.Predicate{
								Left: visitor.Column{
									Name: "b",
								},
								Op: operator.OpAdd,
								Right: visitor.Predicate{
									Left: visitor.Column{
										Name: "a",
									},
									Op:    operator.OpAdd,
									Right: visitor.ValueOf(1),
								},
							},
							Op:    operator.OpMulti,
							Right: visitor.ValueOf(10),
						},
					},
				},
			},
		},
	}
	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			root := ast.Parse(tc.sql).Root
			updateV := &UpdateVisitor{
				BaseVisitor: &BaseVisitor{},
			}
			resp := updateV.Visit(root)
			res := resp.(BaseVal)
			assert.Equal(t, tc.wantErr, res.Err)
			if res.Err != nil {
				return
			}
			actual := res.Data.(UpdateVal)
			assert.Equal(t, tc.wantVal, actual)
		})
	}

}

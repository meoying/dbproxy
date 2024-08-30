package ast

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestHintVisitor(t *testing.T) {
	testcases := []struct {
		name    string
		sql     string
		wantVal Hints
	}{
		{
			name: "SELECT的hint语法",
			sql:  "SELECT /* @proxy k1=true;k2=222 */   * FROM users WHERE (user_id = 1) or (user_id =2);",
			wantVal: Hints{
				"k1": {
					Val: "true",
				},
				"k2": {
					Val: "222",
				},
			},
		},
		{
			name: "update的hint语法",
			sql:  "UPDATE /* @proxy k1=true;k2=222 */  `report` SET `handler_uid` = 123456, `status` = 1 WHERE `id` = 1;",
			wantVal: Hints{
				"k1": {
					Val: "true",
				},
				"k2": {
					Val: "222",
				},
			},
		},
		{
			name: "insert的hint语法",
			sql:  "INSERT /* @proxy k1=true;k2=222 */  INTO `report` (`biz_id`, `biz`, `uid`) VALUES (1001, 'user_report', 2001);",
			wantVal: Hints{
				"k1": {
					Val: "true",
				},
				"k2": {
					Val: "222",
				},
			},
		},
		{
			name: "delete的hint语法",
			sql:  "DELETE /* @proxy k1=true;k2=222 */  FROM `report` WHERE `id` = 1;",
			wantVal: Hints{
				"k1": {
					Val: "true",
				},
				"k2": {
					Val: "222",
				},
			},
		},
		{
			name: "begin的hint语法",
			sql:  "begin /* @proxy k1=true;k2=222 */ ",
			wantVal: Hints{
				"k1": {
					Val: "true",
				},
				"k2": {
					Val: "222",
				},
			},
		},
		{
			name: "commit的hint语法",
			sql:  "commit /* @proxy k1=true;k2=222 */ ",
			wantVal: Hints{
				"k1": {
					Val: "true",
				},
				"k2": {
					Val: "222",
				},
			},
		},
		{
			name: "rollback的hint语法",
			sql:  "rollback /* @proxy k1=true;k2=222 */ ",
			wantVal: Hints{
				"k1": {
					Val: "true",
				},
				"k2": {
					Val: "222",
				},
			},
		},
		{
			name: "SELECT的hint语法前后有空格",
			sql:  "SELECT /* @proxy k1 = true ; k2 = 222 */   * FROM users WHERE (user_id = 1) or (user_id =2);",
			wantVal: Hints{
				"k1": {
					Val: "true",
				},
				"k2": {
					Val: "222",
				},
			},
		},
	}
	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			root := Parse(tc.sql).Root
			hint := NewHintVisitor().Visit(root)
			assert.Equal(t, tc.wantVal, hint)
		})
	}
}

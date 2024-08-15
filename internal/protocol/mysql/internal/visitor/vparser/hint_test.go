package vparser

import (
	"github.com/meoying/dbproxy/internal/protocol/mysql/internal/ast"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestHintVisitor(t *testing.T) {
	testcases := []struct {
		name    string
		sql     string
		wantVal map[string]HintValue
	}{
		{
			name: "SELECT的hint语法",
			sql:  "SELECT /* @proxy k1=v1;k2=v2  */   * FROM users WHERE (user_id = 1) or (user_id =2);",
			wantVal: map[string]HintValue{
				"k1": {
					Key:   "k1",
					Value: "v1",
				},
				"k2": {
					Key:   "k2",
					Value: "v2",
				},
			},
		},
		{
			name:    "select没有hint语法",
			sql:     "SELECT    * FROM users WHERE (user_id = 1) or (user_id =2);",
			wantVal: map[string]HintValue{},
		},
		{
			name: "update的hint语法",
			sql:  "UPDATE /* @proxy k1=v1;k2=v2 */ `report` SET `handler_uid` = 123456, `status` = 1 WHERE `id` = 1;",
			wantVal: map[string]HintValue{
				"k1": {
					Key:   "k1",
					Value: "v1",
				},
				"k2": {
					Key:   "k2",
					Value: "v2",
				},
			},
		},
		{
			name:    "update没有hint语法",
			sql:     "UPDATE `report` SET `handler_uid` = 123456, `status` = 1 WHERE `id` = 1;",
			wantVal: map[string]HintValue{},
		},
		{
			name: "insert的hint语法",
			sql:  "INSERT /* @proxy k1=v1;k2=v2 */ INTO `report` (`biz_id`, `biz`, `uid`) VALUES (1001, 'user_report', 2001);",
			wantVal: map[string]HintValue{
				"k1": {
					Key:   "k1",
					Value: "v1",
				},
				"k2": {
					Key:   "k2",
					Value: "v2",
				},
			},
		},
		{
			name:    "insert的没有hint语法",
			sql:     "INSERT  INTO `report` (`biz_id`, `biz`, `uid`) VALUES (1001, 'user_report', 2001);",
			wantVal: map[string]HintValue{},
		},
		{
			name: "delete的hint语法",
			sql:  "DELETE /* @proxy k1=v1;k2=v2 */ FROM `report` WHERE `id` = 1;",
			wantVal: map[string]HintValue{
				"k1": {
					Key:   "k1",
					Value: "v1",
				},
				"k2": {
					Key:   "k2",
					Value: "v2",
				},
			},
		},
		{
			name:    "delete的没有hint语法",
			sql:     "DELETE FROM `report` WHERE `id` = 1;",
			wantVal: map[string]HintValue{},
		},
		{
			name: "begin的hint语法",
			sql:  "begin /* @proxy k1=v1;k2=v2 */;",
			wantVal: map[string]HintValue{
				"k1": {
					Key:   "k1",
					Value: "v1",
				},
				"k2": {
					Key:   "k2",
					Value: "v2",
				},
			},
		},
		{
			name: "commit的hint语法",
			sql:  "commit /* @proxy k1=v1;k2=v2 */;",
			wantVal: map[string]HintValue{
				"k1": {
					Key:   "k1",
					Value: "v1",
				},
				"k2": {
					Key:   "k2",
					Value: "v2",
				},
			},
		},
		{
			name: "rollback的hint语法",
			sql:  "rollback /* @proxy k1=true;k2=v2 */;",
			wantVal: map[string]HintValue{
				"k1": {
					Key:   "k1",
					Value: "true",
				},
				"k2": {
					Key:   "k2",
					Value: "v2",
				},
			},
		},
	}
	for _, tc := range testcases {
		t.Run(tc.name, func(t *testing.T) {
			root := ast.Parse(tc.sql)
			hint := NewHintVisitor().Visit(root)
			assert.Equal(t, tc.wantVal, hint)
		})
	}
}

package config

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestTemplate_Evaluate(t *testing.T) {
	tests := []struct {
		name          string
		tmpl          *TemplateType
		expected      []string
		assertErrFunc assert.ErrorAssertionFunc
	}{
		{
			name: "无占位符",
			tmpl: &TemplateType{
				Expr: "meoying.com:3306",
			},
			expected: []string{
				"meoying.com:3306",
			},
			assertErrFunc: assert.NoError,
		},
		{
			name: "单个占位符_枚举值",
			tmpl: &TemplateType{
				Expr: "${region}.meoying.com:3306",
				Placeholders: map[string]Placeholder{
					"region": {
						Enum: []string{"us", "pr"},
					},
				},
			},
			expected: []string{
				"us.meoying.com:3306",
				"pr.meoying.com:3306",
			},
			assertErrFunc: assert.NoError,
		},
		{
			name: "单个占位符_对象值",
			tmpl: &TemplateType{
				Expr: "${region}.meoying.com:3306",
				Placeholders: map[string]Placeholder{
					"region": {
						Enum: []string{"us", "pr"},
					},
				},
			},
			expected: []string{
				"us.meoying.com:3306",
				"pr.meoying.com:3306",
			},
			assertErrFunc: assert.NoError,
		},

		{
			name: "多个占位符_混合类型",
			tmpl: &TemplateType{
				Expr: "${region}.${role.}meoying.com:3306",
				Placeholders: map[string]Placeholder{
					"region": {
						Enum: []string{"cn", "hk"},
					},
					"role": {
						Objects: []Object{
							{Key: ""},
							{Key: "slave"},
							{Key: "shadow"},
						},
					},
				},
			},
			expected: []string{
				"cn.meoying.com:3306",
				"cn.slave.meoying.com:3306",
				"cn.shadow.meoying.com:3306",
				"hk.meoying.com:3306",
				"hk.slave.meoying.com:3306",
				"hk.shadow.meoying.com:3306",
			},
			assertErrFunc: assert.NoError,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual, err := tt.tmpl.Evaluate()
			tt.assertErrFunc(t, err)
			if err != nil {
				return
			}
			assert.ElementsMatch(t, tt.expected, actual)
		})
	}
}

// func TestTemplate_EvaluateWith(t *testing.T) {
// 	tests := []struct {
// 		name          string
// 		tmpl          *TemplateType
// 		keys          map[string]string
// 		expected      []string
// 		assertErrFunc assert.ErrorAssertionFunc
// 	}{
// 		{
// 			name: "With region=cn",
// 			tmpl: &TemplateType{
// 				Expr: "${region}.${role}meoying.com:3306",
// 				Placeholders: map[string]Placeholder{
// 					"region": {
// 						Enum: []string{"cn", "hk"},
// 					},
// 					"role": {
// 						Objects: []Object{
// 							{Key: ""},
// 							{Key: "slave"},
// 							{Key: "shadow"},
// 						},
// 					},
// 				},
// 			},
// 			keys: map[string]string{"region": "cn"},
// 			expected: []string{
// 				"cn.meoying.com:3306",
// 				"cn.slave.meoying.com:3306",
// 				"cn.shadow.meoying.com:3306",
// 			},
// 			assertErrFunc: assert.NoError,
// 		},
// 		{
// 			name: "With partial placeholders",
// 			tmpl: &TemplateType{
// 				Expr: "${env}-${service}-${region}",
// 				Placeholders: map[string]Placeholder{
// 					"env":     {Enum: []string{"dev", "prod"}},
// 					"service": {Enum: []string{"web", "api"}},
// 					"region":  {Enum: []string{"us", "eu"}},
// 				},
// 			},
// 			keys: map[string]string{"env": "dev", "region": "us"},
// 			expected: []string{
// 				"dev-web-us",
// 				"dev-api-us",
// 			},
// 			assertErrFunc: assert.NoError,
// 		},
// 	}
//
// 	for _, tt := range tests {
// 		t.Run(tt.name, func(t *testing.T) {
// 			actual, err := tt.tmpl.EvaluateWith(tt.keys)
// 			tt.assertErrFunc(t, err)
// 			if err != nil {
// 				return
// 			}
// 			assert.ElementsMatch(t, tt.expected, actual)
// 		})
// 	}
// }

func TestTemplate_EvaluateWith(t *testing.T) {

	tests := []struct {
		name                string
		tmpl                *TemplateType
		partialPlaceholders map[string][]interface{}
		expected            []string
		assertErrFunc       assert.ErrorAssertionFunc
	}{
		{
			name: "部分占位符值",
			tmpl: &TemplateType{
				Expr: "${region}-${count}-${enabled}.meoying.com:3306",
				Placeholders: map[string]Placeholder{
					"region": {
						Enum: Enum{"cn", "hk", "us"},
					},
					"count": {
						Objects: []Object{
							{Key: "1"},
							{Key: "2"},
							{Key: "3"},
						},
					},
					"enabled": {
						Enum: Enum{"true", "false"},
					},
				},
			},
			partialPlaceholders: map[string][]interface{}{
				"region":  {"cn", "hk"},
				"count":   {1, 2},
				"enabled": {true},
			},
			expected: []string{
				"cn-1-true.meoying.com:3306",
				"cn-2-true.meoying.com:3306",
				"hk-1-true.meoying.com:3306",
				"hk-2-true.meoying.com:3306",
			},
			assertErrFunc: assert.NoError,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual, err := tt.tmpl.EvaluateWith(tt.partialPlaceholders)
			tt.assertErrFunc(t, err)
			if err != nil {
				return
			}
			assert.ElementsMatch(t, tt.expected, actual)
		})
	}
}

// func TestTemplate_Search(t *testing.T) {
// 	tests := []struct {
// 		name          string
// 		tmpl          *TemplateType
// 		filter        map[string]string
// 		expected      []string
// 		assertErrFunc assert.ErrorAssertionFunc
// 	}{
// 		{
// 			name: "Search with region=cn",
// 			tmpl: &TemplateType{
// 				Expr: "${region}.${role}meoying.com:3306",
// 				Placeholders: map[string]Placeholder{
// 					"region": {
// 						Enum: []string{"cn", "hk"},
// 					},
// 					"role": {
// 						Objects: []Object{
// 							{Key: ""},
// 							{Key: "slave"},
// 							{Key: "shadow"},
// 						},
// 					},
// 				},
// 			},
// 			filter: map[string]string{"region": "cn"},
// 			expected: []string{
// 				"cn.meoying.com:3306",
// 				"cn.slave.meoying.com:3306",
// 				"cn.shadow.meoying.com:3306",
// 			},
// 			assertErrFunc: assert.NoError,
// 		},
// 		{
// 			name: "Search with empty filter",
// 			tmpl: &TemplateType{
// 				Expr: "${service}-${version}",
// 				Placeholders: map[string]Placeholder{
// 					"service": {Enum: []string{"app", "api"}},
// 					"version": {Enum: []string{"v1", "v2"}},
// 				},
// 			},
// 			filter: map[string]string{},
// 			expected: []string{
// 				"app-v1",
// 				"app-v2",
// 				"api-v1",
// 				"api-v2",
// 			},
// 			assertErrFunc: assert.NoError,
// 		},
// 	}
//
// 	for _, tt := range tests {
// 		t.Run(tt.name, func(t *testing.T) {
// 			actual, err := tt.tmpl.Search(tt.filter)
// 			tt.assertErrFunc(t, err)
// 			if err != nil {
// 				return
// 			}
// 			assert.ElementsMatch(t, tt.expected, actual)
// 		})
// 	}
// }

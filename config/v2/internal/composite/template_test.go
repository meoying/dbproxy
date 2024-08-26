package composite

import (
	"testing"

	"github.com/meoying/dbproxy/config/v2/internal/errs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

type TemplateConfig struct {
	Templ Template `yaml:"template"`
}

func TestTemplate(t *testing.T) {
	tests := []struct {
		name     string
		yamlData string

		want        Template
		assertError assert.ErrorAssertionFunc
	}{
		{
			name: "占位符为字符串类型",
			yamlData: `
template:
  expr: ${region}.order_db
  placeholders:
    region: hk`,
			want: Template{
				Expr: "${region}.order_db",
				Placeholders: Placeholders{
					Variables: map[string]Placeholder{
						"region": {String: String("hk")},
					},
				},
			},
			assertError: assert.NoError,
		},
		{
			name: "占位符为数组类型",
			yamlData: `
template:
  expr: ${region}.order_db
  placeholders:
    region:
      - us
      - uk
`,
			want: Template{
				Expr: "${region}.order_db",
				Placeholders: Placeholders{
					Variables: map[string]Placeholder{
						"region": {Enum: Enum{"us", "uk"}},
					},
				},
			},
			assertError: assert.NoError,
		},
		{
			name: "占位符为哈希类型",
			yamlData: `
template:
  expr: order_db_${key}
  placeholders:
    key:
      hash:
        key: user_id
        base: 3
`,
			want: Template{
				Expr: "order_db_${key}",
				Placeholders: Placeholders{
					Variables: map[string]Placeholder{
						"key": {Hash: Hash{
							Key:  "user_id",
							Base: 3,
						}},
					},
				},
			},
			assertError: assert.NoError,
		},
		// TODO: 占位符为引用类型
		// 1) 引用字符串
		// 2) 引用枚举
		// 3) 引用哈希
		// TODO: 应该报错_引用模版
		{
			name: "占位符为各种类型的组合",
			yamlData: `
template:
  expr: ${region}.${role}.${type}.${id}.example.com
  placeholders:
    region:
      - cn
      - us
    role:
      - master
      - slave
    type: mysql
    id:
      hash:
        key: user_id
        base: 3
`,
			want: Template{
				Expr: "${region}.${role}.${type}.${id}.example.com",
				Placeholders: Placeholders{
					Variables: map[string]Placeholder{
						"region": {Enum: Enum{"cn", "us"}},
						"role":   {Enum: Enum{"master", "slave"}},
						"type":   {String: String("mysql")},
						"id": {Hash: Hash{
							Key:  "user_id",
							Base: 3,
						}},
					},
				},
			},
			assertError: assert.NoError,
		},
		{
			name: "解析失败_表达式为空",
			yamlData: `
template:
  expr:
  placeholders:
    region: hk`,
			want: Template{},
			assertError: func(t assert.TestingT, err error, i ...interface{}) bool {
				return assert.ErrorIs(t, err, errs.ErrUnmarshalVariableFailed)
			},
		},
		{
			name: "解析失败_占位符定义列表为空",
			yamlData: `
template:
  expr: ${key}.order_db
  placeholders:
`,
			want: Template{},
			assertError: func(t assert.TestingT, err error, i ...interface{}) bool {
				return assert.ErrorIs(t, err, errs.ErrUnmarshalVariableFailed)
			},
		},
		{
			name: "解析失败_表达式中无占位符",
			yamlData: `
template:
  expr: order_db
  placeholders:
    region: hk`,
			want: Template{},
			assertError: func(t assert.TestingT, err error, i ...interface{}) bool {
				return assert.ErrorIs(t, err, errs.ErrUnmarshalVariableFailed)
			},
		},
		{
			name: "解析失败_表达式中占位符与定义的不匹配",
			yamlData: `
template:
  expr: ${key}.order_db
  placeholders:
    region: hk`,
			want: Template{},
			assertError: func(t assert.TestingT, err error, i ...interface{}) bool {
				return assert.ErrorIs(t, err, errs.ErrUnmarshalVariableFailed)
			},
		},
		{
			name: "解析失败_表达式中占位符与定义的不匹配_列表中多一个",
			yamlData: `
template:
  expr: ${key}.order_db
  placeholders:
    key: mysql
    region: hk`,
			want: Template{},
			assertError: func(t assert.TestingT, err error, i ...interface{}) bool {
				return assert.ErrorIs(t, err, errs.ErrUnmarshalVariableFailed)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var cfg TemplateConfig
			err := yaml.Unmarshal([]byte(tt.yamlData), &cfg)
			tt.assertError(t, err)
			if err != nil {
				return
			}
			assert.Equal(t, tt.want, cfg.Templ)
		})
	}
}

func TestTemplate_Evaluate(t *testing.T) {
	tests := []struct {
		name     string
		yamlData string

		wantValues  map[string]string
		assertError assert.ErrorAssertionFunc
	}{
		{
			name: "占位符为字符串类型",
			yamlData: `
template:
  expr: ${region}.order_db
  placeholders:
    region: hk`,
			wantValues: map[string]string{
				"hk": "hk.order_db",
			},
			assertError: assert.NoError,
		},
		{
			name: "占位符为数组类型",
			yamlData: `
template:
  expr: ${region}.order_db
  placeholders:
    region:
      - us
      - uk
`,
			wantValues: map[string]string{
				"us": "us.order_db",
				"uk": "uk.order_db",
			},
			assertError: assert.NoError,
		},
		{
			name: "占位符为哈希类型",
			yamlData: `
template:
  expr: order_db_${key}
  placeholders:
    key:
      hash:
        key: user_id
        base: 3
`,
			wantValues: map[string]string{
				"0": "order_db_0",
				"1": "order_db_1",
				"2": "order_db_2",
			},
			assertError: assert.NoError,
		},
		{
			name: "占位符为各种类型的组合",
			yamlData: `
template:
  expr: ${region}.${role}.${type}.${id}.example.com
  placeholders:
    region:
      - cn
      - us
    role:
      - master
      - slave
    type: mysql
    id:
      hash:
        key: user_id
        base: 3
`,
			wantValues: map[string]string{
				"cn_master_mysql_0": "cn.master.mysql.0.example.com",
				"cn_master_mysql_1": "cn.master.mysql.1.example.com",
				"cn_master_mysql_2": "cn.master.mysql.2.example.com",
				"cn_slave_mysql_0":  "cn.slave.mysql.0.example.com",
				"cn_slave_mysql_1":  "cn.slave.mysql.1.example.com",
				"cn_slave_mysql_2":  "cn.slave.mysql.2.example.com",
				"us_master_mysql_0": "us.master.mysql.0.example.com",
				"us_master_mysql_1": "us.master.mysql.1.example.com",
				"us_master_mysql_2": "us.master.mysql.2.example.com",
				"us_slave_mysql_0":  "us.slave.mysql.0.example.com",
				"us_slave_mysql_1":  "us.slave.mysql.1.example.com",
				"us_slave_mysql_2":  "us.slave.mysql.2.example.com",
			},
			assertError: assert.NoError,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {

			var cfg TemplateConfig
			err := yaml.Unmarshal([]byte(tt.yamlData), &cfg)
			require.NoError(t, err)

			evaluate, err := cfg.Templ.Evaluate()
			tt.assertError(t, err)
			if err != nil {
				return
			}
			assert.Equal(t, tt.wantValues, evaluate)
		})
	}
}

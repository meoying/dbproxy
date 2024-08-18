package v1

import (
	"log"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

// TODO: 模版类型嵌套模版
//         即占位符的类型是另一个模版类型, 或者通过引用类型间接引用的模版类型
// TODO: 引用类型
//         层次问题, 循环引用问题, a -> b -> c -> a
//         引用模版类型
//            模版类型中还有引用, 会有循环引用问题, 链路过长问题
//         校验引用多个变量时, 类型不一致问题 - variables.region 和 variables.hash,
//         交叉引用, variables 下的变量引用 databases, datasources, tables 下的变量

func TestConfig_UnmarshalYAML(t *testing.T) {
	tests := []struct {
		name        string
		yamlData    string
		assertError assert.ErrorAssertionFunc
	}{
		// 引用类型
		{
			name: "反序列化失败_引用类型_变量类型错误",
			yamlData: `
variables:
  str_value: hello world
  err_ref:
    refs: error.string`,
			assertError: func(t assert.TestingT, err error, i ...interface{}) bool {
				return assert.ErrorIs(t, err, ErrVariableTypeInvalid)
			},
		},
		{
			name: "反序列化失败_引用类型_变量值类型错误",
			yamlData: `
variables:
  str_value: hello world
  err_ref:
    ref: error.string`,
			assertError: func(t assert.TestingT, err error, i ...interface{}) bool {
				return assert.ErrorIs(t, err, ErrVariableTypeInvalid)
			},
		},
		// 模版类型
		{
			name: "反序列化失败_模板类型_expr为空",
			yamlData: `
variables:
  tmpl_value:
    template:
      expr: 
      placeholders:
        placeholder1:
          - value1
          - value2`,
			assertError: func(t assert.TestingT, err error, i ...interface{}) bool {
				log.Printf("err = %#v\n", err)
				return assert.ErrorIs(t, err, ErrVariableTypeInvalid)
			},
		},
		{
			name: "反序列化失败_模板类型_expr为空字符串",
			yamlData: `
variables:
  tmpl_value:
    template:
      expr: ""
      placeholders:
        placeholder1:
          - value1
          - value2`,
			assertError: func(t assert.TestingT, err error, i ...interface{}) bool {
				log.Printf("err = %#v\n", err)
				return assert.ErrorIs(t, err, ErrVariableTypeInvalid)
			},
		},
		{
			name: "反序列化失败_模板类型_placeholders属性值类型错误",
			yamlData: `
variables:
  tmpl_value:
    template:
      expr: "hello"
      placeholders: world`,
			assertError: func(t assert.TestingT, err error, i ...interface{}) bool {
				log.Printf("err = %#v\n", err)
				return assert.ErrorIs(t, err, ErrVariableTypeInvalid)
			},
		},
		{
			name: "反序列化失败_模板类型_placeholders属性为空",
			yamlData: `
variables:
  tmpl_value:
    template:
      expr: "hello"
      placeholders:
`,
			assertError: func(t assert.TestingT, err error, i ...interface{}) bool {
				log.Printf("err = %#v\n", err)
				return assert.ErrorIs(t, err, ErrVariableTypeInvalid)
			},
		},
		{
			name: "反序列化失败_模板类型_expr中无通配符",
			yamlData: `
variables:
  tmpl_value:
    template:
      expr: "hello world"
      placeholders:
        placeholder1:
          - value1
          - value2`,
			assertError: func(t assert.TestingT, err error, i ...interface{}) bool {
				log.Printf("err = %#v\n", err)
				return assert.ErrorIs(t, err, ErrVariableTypeInvalid)
			},
		},
		{
			name: "反序列化失败_模板类型_expr与placeholders不匹配",
			yamlData: `
variables:
  tmpl_value:
    template:
      expr: "${placeholder1} - ${key}"
      placeholders:
        placeholder1:
          - value1
          - value2
      k:
        `,
			assertError: func(t assert.TestingT, err error, i ...interface{}) bool {
				log.Printf("err = %#v\n", err)
				return assert.ErrorIs(t, err, ErrVariableTypeInvalid)
			},
		},
		{
			name: "反序列化失败_模板类型_属性名错误",
			yamlData: `
variables:
  tmpl_value:
    template:
      expression: "${placeholder1}.example.com"
      placeholders:
        placeholder1:
          - value1
          - value2`,
			assertError: func(t assert.TestingT, err error, i ...interface{}) bool {
				log.Printf("err = %#v\n", err)
				return assert.ErrorIs(t, err, ErrVariableTypeInvalid)
			},
		},
		// 哈希类型
		{
			name: "反序列化失败_哈希类型_key为空",
			yamlData: `
variables:
  hash_value:
    hash:
      key: 
      base: 10`,
			assertError: func(t assert.TestingT, err error, i ...interface{}) bool {
				log.Printf("err = %#v\n", err)
				return assert.ErrorIs(t, err, ErrVariableTypeInvalid)
			},
		},
		{
			name: "反序列化失败_哈希类型_base为空",
			yamlData: `
variables:
  hash_value:
    hash:
      key: user_id
      base: 0`,
			assertError: func(t assert.TestingT, err error, i ...interface{}) bool {
				log.Printf("err = %#v\n", err)
				return assert.ErrorIs(t, err, ErrVariableTypeInvalid)
			},
		},
		{
			name: "反序列化失败_哈希类型_base为负数",
			yamlData: `
variables:
  hash_value:
    hash:
      key: user_id
      base: -1`,
			assertError: func(t assert.TestingT, err error, i ...interface{}) bool {
				log.Printf("err = %#v\n", err)
				return assert.ErrorIs(t, err, ErrVariableTypeInvalid)
			},
		},
		// datasource 数据类型
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var config Config
			err := yaml.Unmarshal([]byte(tt.yamlData), &config)
			tt.assertError(t, err)
		})
	}
}

func TestConfig_GetVariableByName(t *testing.T) {
	tests := []struct {
		name         string
		yamlData     string
		varName      string
		getWantValue func(t *testing.T, config *Config) any
		assertError  assert.ErrorAssertionFunc
	}{
		{
			name: "反序列化成功_不存在的值",
			yamlData: `
variables:
  existing_value: some value`,
			varName: "non_existing_value",
			getWantValue: func(t *testing.T, config *Config) any {
				t.Helper()
				return nil
			},
			assertError: func(t assert.TestingT, err error, msgAndArgs ...any) bool {
				return assert.ErrorIs(t, err, ErrVariableNameNotFound)
			},
		},
		{
			name: "反序列化成功_字符串类型",
			yamlData: `
variables:
  str_value: hello world`,
			varName: "str_value",
			getWantValue: func(t *testing.T, config *Config) any {
				t.Helper()
				return String("hello world")
			},
			assertError: assert.NoError,
		},
		// 补充其他类型
		{
			name: "反序列化成功_枚举类型",
			yamlData: `
variables:
  enum_value:
    - item1
    - item2
    - item3`,
			varName: "enum_value",
			getWantValue: func(t *testing.T, config *Config) any {
				t.Helper()
				return Enum{"item1", "item2", "item3"}
			},
			assertError: assert.NoError,
		},
		{
			name: "反序列化成功_引用类型",
			yamlData: `
variables:
  str_value: hello world
  ref_value:
    ref:
      - variables.str_value`,
			varName: "ref_value",
			getWantValue: func(t *testing.T, config *Config) any {
				t.Helper()
				return &Reference{
					values: map[string]any{
						"variables.str_value": String("hello world"),
					},
					config: config,
				}
			},
			assertError: assert.NoError,
		},
		{
			name: "反序列化成功_引用类型_等价写法",
			yamlData: `
variables:
  str_value: hello world
  ref_value:
    ref: [variables.str_value]`,
			varName: "ref_value",
			getWantValue: func(t *testing.T, config *Config) any {
				t.Helper()
				return &Reference{
					values: map[string]any{
						"variables.str_value": String("hello world"),
					},
					config: config,
				}
			},
			assertError: assert.NoError,
		},
		{
			name: "反序列化成功_哈希类型",
			yamlData: `
variables:
  hash_value:
    hash:
      key: user_id
      base: 10`,
			varName: "hash_value",
			getWantValue: func(t *testing.T, config *Config) any {
				t.Helper()
				return &Hash{
					Key:  "user_id",
					Base: 10,
				}
			},
			assertError: assert.NoError,
		},
		{
			name: "反序列化成功_模板类型",
			yamlData: `
variables:
  region1:
    - cn
    - hk
  region2:
    - us
    - uk
  tmpl_value:
    template:
      expr: "${enum}.${str}.${ref}.${str}.${hash}.example.com"
      placeholders:
        enum:
          - value1
          - value2
        str: "str"
        ref:
          ref:
            - variables.region1
            - variables.region2
        hash:
          hash:
            key: user_id
            base: 32`,
			varName: "tmpl_value",
			getWantValue: func(t *testing.T, config *Config) any {
				t.Helper()
				return &Template{
					Expr: "${enum}.${str}.${ref}.${str}.${hash}.example.com",
					Placeholders: map[string]any{
						"enum": Enum{"value1", "value2"},
						"str":  String("str"),
						"ref": &Reference{
							values: map[string]any{
								"variables.region1": Enum{"cn", "hk"},
								"variables.region2": Enum{"us", "uk"},
							},
							config: config,
						},
						"hash": &Hash{
							Key:  "user_id",
							Base: 32,
						},
					},
					config: config,
				}
			},
			assertError: assert.NoError,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var config Config
			err := yaml.Unmarshal([]byte(tt.yamlData), &config)
			require.NoError(t, err)

			actualValue, err := config.GetVariableByName(tt.varName)
			tt.assertError(t, err)
			if err != nil {
				return
			}

			expectedValue := tt.getWantValue(t, &config)
			log.Printf("expected = %#v\n", expectedValue)
			log.Printf("actual = %#v\n", actualValue)
			assert.Equal(t, expectedValue, actualValue)
		})
	}
}

func TestEvaluate(t *testing.T) {
	tests := []struct {
		name        string
		eval        stringEvaluator
		want        []string
		assertError assert.ErrorAssertionFunc
	}{
		{
			name:        "字符串类型",
			eval:        String("go"),
			want:        []string{"go"},
			assertError: assert.NoError,
		},

		{
			name:        "枚举类型",
			eval:        Enum{"cn", "hk"},
			want:        []string{"cn", "hk"},
			assertError: assert.NoError,
		},
		{
			name: "哈希类型",
			eval: &Hash{
				Key:  "user_id",
				Base: 8,
			},
			want:        []string{"0", "1", "2", "3", "4", "5", "6", "7"},
			assertError: assert.NoError,
		},
		{
			name: "引用类型_字符串",
			eval: &Reference{
				values: map[string]any{
					"str1": String("go"),
					"str2": String("py"),
				},
			},
			want:        []string{"go", "py"},
			assertError: assert.NoError,
		},
		{
			name: "引用类型_枚举",
			eval: &Reference{
				values: map[string]any{
					"enum1": Enum{"value1", "value2"},
					"enum2": Enum{"value3", "value4"},
				},
			},
			want:        []string{"value1", "value2", "value3", "value4"},
			assertError: assert.NoError,
		},
		{
			name: "引用类型_哈希",
			eval: &Reference{
				values: map[string]any{
					"hash": &Hash{
						Key:  "user_id",
						Base: 3,
					},
				},
			},
			want:        []string{"0", "1", "2"},
			assertError: assert.NoError,
		},
		// 引用自己?
		// 引用嵌套?
		{
			name: "模版类型_引用哈希类型",
			eval: &Template{
				Expr: "order_db_${key}",
				Placeholders: map[string]any{
					"key": &Reference{
						values: map[string]any{
							"variables.db_key": &Hash{
								Key:  "user_id",
								Base: 3,
							},
						},
					},
				},
			},
			want:        []string{"order_db_0", "order_db_1", "order_db_2"},
			assertError: assert.NoError,
		},
		{
			name: "模版类型_组合",
			eval: &Template{
				Expr: "${region}.${role}.${type}.${hash}.example.com",
				Placeholders: map[string]any{
					"region": &Reference{
						values: map[string]any{
							"values.region1": Enum{"cn"},
							"values.region2": Enum{"us"},
						},
					},
					"role": Enum{"master", "slave"},
					"type": String("mysql"),
					"hash": &Hash{
						Key:  "user_id",
						Base: 3,
					},
				},
			},
			want: []string{
				"cn.master.mysql.0.example.com",
				"cn.master.mysql.1.example.com",
				"cn.master.mysql.2.example.com",
				"cn.slave.mysql.0.example.com",
				"cn.slave.mysql.1.example.com",
				"cn.slave.mysql.2.example.com",
				"us.master.mysql.0.example.com",
				"us.master.mysql.1.example.com",
				"us.master.mysql.2.example.com",
				"us.slave.mysql.0.example.com",
				"us.slave.mysql.1.example.com",
				"us.slave.mysql.2.example.com",
			},
			assertError: assert.NoError,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual, err := tt.eval.Evaluate()
			tt.assertError(t, err)
			if err != nil {
				return
			}
			assert.ElementsMatch(t, tt.want, actual)
		})
	}

}

func TestConfig_GetDatabaseByName(t *testing.T) {

}

func TestConfig_GetDatasourceByName(t *testing.T) {
	tests := []struct {
		name         string
		yamlData     string
		dsName       string
		getWantValue func(t *testing.T, config *Config) any
		assertError  assert.ErrorAssertionFunc
	}{
		{
			name: "反序列化成功_不存在的值",
			yamlData: `
datasources:
  existing_value: some value`,
			dsName: "non_existing_value",
			getWantValue: func(t *testing.T, config *Config) any {
				t.Helper()
				return nil
			},
			assertError: func(t assert.TestingT, err error, msgAndArgs ...any) bool {
				return assert.ErrorIs(t, err, ErrVariableNameNotFound)
			},
		},
		{
			name: "反序列化成功_单个",
			yamlData: `
datasources:
  dbproxy_ds:
    - datasource:
        master: webook:134root@tcp(cn.meoying.com:3306)/?charset=utf8mb4&parseTime=True
        slave:
          template:
            expr: webook:${password}@tcp(${region}.${id}.slave.meoying.com:3306)/?charset=utf8mb4&parseTime=True
            placeholders:
              password: webook
              region:
                - cn 
                - hk
              id:
                hash:
                  key: user_id
                  base: 3`,
			dsName: "dbproxy_ds",
			getWantValue: func(t *testing.T, config *Config) any {
				t.Helper()
				return []any{
					&Datasource{
						Master: "webook:134root@tcp(cn.meoying.com:3306)/?charset=utf8mb4&parseTime=True",
						Slave: any(&Template{
							Expr: "webook:${password}@tcp(${region}.${id}.slave.meoying.com:3306)/?charset=utf8mb4&parseTime=True",
							Placeholders: map[string]any{
								"password": String("webook"),
								"region":   Enum{"cn", "hk"},
								"id":       &Hash{Key: "user_id", Base: 3},
							},
							config: config,
						}),
						config: config,
					},
				}
			},
			assertError: assert.NoError,
		},
		{
			name: "反序列化成功_多个",
			yamlData: `
variables:
  password: webook
  region1:
    - cn
    - hk
  region2:
    - us
    - uk
  index:
    hash:
      key: id
      base: 3
datasources:
  dbproxy_ds:
    - datasource:
        master: webook:134root@tcp(cn.meoying.com:3306)/?charset=utf8mb4&parseTime=True
        slave:
          template:
            expr: webook:webook@tcp(${region}.slave.meoying.com:3306)/?charset=utf8mb4&parseTime=True
            placeholders:
              region:
                - cn
                - hk
    - datasource:
        master: webook:webook@tcp(us.meoying.com:3306)/?charset=utf8mb4&parseTime=True
        slave:
          template:
            expr: webook:${password}@tcp(${region}.${type}.slave.meoying.com:3306)/?charset=utf8mb4&parseTime=True
            placeholders:
              password:
                ref:
                  - variables.password
              region:
                ref:
                  - variables.region1
                  - variables.region2
              type:
                ref:
                  - variables.index`,
			dsName: "dbproxy_ds",
			getWantValue: func(t *testing.T, config *Config) any {
				t.Helper()
				return []any{
					&Datasource{
						Master: "webook:134root@tcp(cn.meoying.com:3306)/?charset=utf8mb4&parseTime=True",
						Slave: any(&Template{
							Expr: "webook:webook@tcp(${region}.slave.meoying.com:3306)/?charset=utf8mb4&parseTime=True",
							Placeholders: map[string]any{
								"region": Enum{"cn", "hk"},
							},
							config: config,
						}),
						config: config,
					},
					&Datasource{
						Master: "webook:webook@tcp(us.meoying.com:3306)/?charset=utf8mb4&parseTime=True",
						Slave: any(&Template{
							Expr: "webook:${password}@tcp(${region}.${type}.slave.meoying.com:3306)/?charset=utf8mb4&parseTime=True",
							Placeholders: map[string]any{
								"password": &Reference{
									values: map[string]any{
										"variables.password": String("webook"),
									},
									config: config,
								},
								"region": &Reference{
									values: map[string]any{
										"variables.region1": Enum{"cn", "hk"},
										"variables.region2": Enum{"uk", "us"},
									},
									config: config,
								},
								"type": &Reference{
									values: map[string]any{
										"variables.index": &Hash{Key: "id", Base: 3},
									},
									config: config,
								},
							},
							config: config,
						}),
						config: config,
					},
				}
			},
			assertError: assert.NoError,
		},

		// 不存在
		// 唯恐
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var config Config
			err := yaml.Unmarshal([]byte(tt.yamlData), &config)
			require.NoError(t, err)

			actual, err := config.GetDatasourceByName(tt.dsName)
			tt.assertError(t, err)
			if err != nil {
				return
			}

			expectedVals := tt.getWantValue(t, &config).([]any)
			actualVals := actual.([]any)
			for i := range expectedVals {
				assert.EqualExportedValues(t, expectedVals[i], actualVals[i])
			}
		})
	}
}

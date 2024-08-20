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
// TODO: datasources类型
//          禁止自引用, dbproxy_ds下的1号datasource 引用 datasources.dbproxy_ds.0
// TODO: ref: database.order_ds (map) 与 ref:database.order_ds (str) 是不一样的

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
		varNames     []string
		getWantValue func(t *testing.T, config *Config) []Variable
		assertError  assert.ErrorAssertionFunc
	}{
		// 		{
		// 			name: "反序列化成功_不存在的值",
		// 			yamlData: `
		// variables:
		//   existing_value: some value`,
		// 			varNames: []string{"non_existing_value"},
		// 			getWantValue: func(t *testing.T, config *Config) []Variable {
		// 				t.Helper()
		// 				return nil
		// 			},
		// 			assertError: func(t assert.TestingT, err error, msgAndArgs ...any) bool {
		// 				return assert.ErrorIs(t, err, ErrVariableNameNotFound)
		// 			},
		// 		},
		{
			name: "反序列化成功_字符串类型",
			yamlData: `
variables:
  str_value: hello world`,
			varNames: []string{"str_value"},
			getWantValue: func(t *testing.T, config *Config) []Variable {
				t.Helper()
				return []Variable{
					{
						varName: "str_value",
						varType: DataTypeString,
						Value:   String("hello world"),
						config:  config,
					},
				}
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
			varNames: []string{"enum_value"},
			getWantValue: func(t *testing.T, config *Config) []Variable {
				t.Helper()
				return []Variable{
					{
						varName: "enum_value",
						varType: DataTypeEnum,
						Value:   Enum{"item1", "item2", "item3"},
						config:  config,
					},
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
			varNames: []string{"hash_value"},
			getWantValue: func(t *testing.T, config *Config) []Variable {
				t.Helper()
				return []Variable{
					{
						varName: "hash_value",
						varType: DataTypeHash,
						Value: Hash{
							varName: "hash_value",
							Key:     "user_id",
							Base:    10,
						},
						config: config,
					},
				}
			},
			assertError: assert.NoError,
		},
		{
			name: "反序列化成功_引用类型_字符串类型",
			yamlData: `
variables:
  str_value: hello world
  ref_value:
    ref: variables.str_value`,
			varNames: []string{"ref_value"},
			getWantValue: func(t *testing.T, config *Config) []Variable {
				t.Helper()
				return []Variable{
					{
						varName: "ref_value",
						varType: DataTypeString,
						Value:   String("hello world"),
						config:  config,
					},
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
      expr: "${enum_val}.${str}.${ref_val}.${str}.${hash_val}.example.com"
      placeholders:
        enum_val:
          - value1
          - value2
        str: "str"
        ref_val:
          ref: variables.region1
        hash_val:
          hash:
            key: user_id
            base: 32`,
			varNames: []string{"tmpl_value"},
			getWantValue: func(t *testing.T, config *Config) []Variable {
				t.Helper()
				return []Variable{
					{
						varName: "tmpl_value",
						varType: DataTypeTemplate,
						Value: Template{
							varName: "tmpl_value",
							Expr:    "${enum_val}.${str}.${ref_val}.${str}.${hash_val}.example.com",
							Placeholders: map[string]any{
								"enum_val": Enum{"value1", "value2"},
								"str":      String("str"),
								"ref_val":  Enum{"cn", "hk"},
								"hash_val": Hash{
									varName: "hash_val",
									Key:     "user_id",
									Base:    32,
								},
							},
							config: config,
						},
						config: config,
					},
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

			assert.Subset(t, config.VariableNames(), tt.varNames)

			expectedValues := tt.getWantValue(t, &config)

			for i, varName := range tt.varNames {
				actual, err := config.VariableByName(varName)
				tt.assertError(t, err)
				if err != nil {
					return
				}
				assert.Equal(t, expectedValues[i], actual)
			}
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
			name: "模版类型_引用哈希类型",
			eval: &Template{
				Expr: "order_db_${key}",
				Placeholders: map[string]any{
					"key": Hash{
						Key:  "user_id",
						Base: 3,
					},
				},
			},
			want:        []string{"order_db_0", "order_db_1", "order_db_2"},
			assertError: assert.NoError,
		},
		{
			name: "模版类型_组合",
			eval: &Template{
				Expr: "${region}.${role}.${type}.${id}.example.com",
				Placeholders: map[string]any{
					"region": Enum{"cn", "us"},
					"role":   Enum{"master", "slave"},
					"type":   String("mysql"),
					"id": Hash{
						varName: "hash",
						Key:     "user_id",
						Base:    3,
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
	tests := []struct {
		name         string
		yamlData     string
		varNames     []string
		getWantValue func(t *testing.T, config *Config) []Database
		assertError  assert.ErrorAssertionFunc
	}{
		{
			name: "模版类型",
			yamlData: `
databases:
  tmpl_db:
    template:
      expr: user_db_${key}
      placeholders:
        key:
          hash:
            key: user_id
            base: 10`,
			varNames: []string{"tmpl_db"},
			getWantValue: func(t *testing.T, config *Config) []Database {
				return []Database{
					{
						varName: "tmpl_db",
						varType: DataTypeTemplate,
						Value: Template{
							varName: "tmpl_db",
							Expr:    "user_db_${key}",
							Placeholders: map[string]any{
								"key": Hash{varName: "key", Key: "user_id", Base: 10},
							},
							config: config,
						},
						config: config,
					},
				}
			},
			assertError: assert.NoError,
		},
		{
			name: "字符串类型",
			yamlData: `
databases:
  str_db: user_db`,
			varNames: []string{"str_db"},
			getWantValue: func(t *testing.T, config *Config) []Database {
				return []Database{
					{
						varType: DataTypeString,
						varName: "str_db",
						Value:   String("user_db"),
						config:  config,
					},
				}
			},
			assertError: assert.NoError,
		},
		{
			name: "引用类型",
			yamlData: `
databases:
  ref_db:
    ref: databases.tmpl_db
  tmpl_db:
    template:
      expr: user_db_${key}
      placeholders:
        key:
          hash:
            key: user_id
            base: 10`,
			varNames: []string{"ref_db"},
			getWantValue: func(t *testing.T, config *Config) []Database {
				return []Database{
					{
						varName: "ref_db",
						varType: DataTypeTemplate,
						Value: Template{
							varName: "ref_db",
							Expr:    "user_db_${key}",
							Placeholders: map[string]any{
								"key": Hash{varName: "key", Key: "user_id", Base: 10},
							},
							config: config,
						},
						config: config,
					},
				}
			},
			assertError: assert.NoError,
		},
		{
			name: "组合情况",
			yamlData: `
databases:
  ref_str_db:
    ref: databases.str_db
  ref_tmpl_db:
    ref: databases.tmpl_db
  str_db: user_db
  tmpl_db:
    template:
      expr: user_db_${key}
      placeholders:
        key:
          hash:
            key: user_id
            base: 10`,
			varNames: []string{"tmpl_db", "str_db", "ref_tmpl_db", "ref_str_db"},
			getWantValue: func(t *testing.T, config *Config) []Database {
				return []Database{
					{
						varName: "tmpl_db",
						varType: DataTypeTemplate,
						Value: Template{
							varName: "tmpl_db",
							Expr:    "user_db_${key}",
							Placeholders: map[string]any{
								"key": Hash{varName: "key", Key: "user_id", Base: 10},
							},
							config: config,
						},
						config: config,
					},
					{
						varType: DataTypeString,
						varName: "str_db",
						Value:   String("user_db"),
						config:  config,
					},
					{
						varName: "ref_tmpl_db",
						varType: DataTypeTemplate,
						Value: Template{
							varName: "ref_tmpl_db",
							Expr:    "user_db_${key}",
							Placeholders: map[string]any{
								"key": Hash{varName: "key", Key: "user_id", Base: 10},
							},
							config: config,
						},
						config: config,
					},
					{
						varType: DataTypeString,
						varName: "ref_str_db",
						Value:   String("user_db"),
						config:  config,
					},
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

			// assert.Contains(t, config.DatabaseNames(), tt.varNames)
			assert.Subset(t, config.DatabaseNames(), tt.varNames)

			expectedValues := tt.getWantValue(t, &config)

			for i, varName := range tt.varNames {
				actual, err := config.DatabaseByName(varName)
				tt.assertError(t, err)
				if err != nil {
					return
				}
				assert.Equal(t, expectedValues[i], actual)
			}

			// assert.Equal(t, tt.getWantValue(t, &config).(*Database).Value.(*Template), actual.(*Database).Value.(*Template))
		})
	}
}

func TestConfig_GetDatasourceByName(t *testing.T) {
	tests := []struct {
		name         string
		yamlData     string
		varNames     []string
		getWantValue func(t *testing.T, config *Config) []Datasource
		assertError  assert.ErrorAssertionFunc
	}{
		// 		{
		// 			name: "反序列化成功_不存在的值",
		// 			yamlData: `
		// datasources:
		//   existing_value: some value`,
		// 			varNames: []string{"non_existing_value"},
		// 			getWantValue: func(t *testing.T, config *Config) []Datasource {
		// 				t.Helper()
		// 				return nil
		// 			},
		// 			assertError: func(t assert.TestingT, err error, msgAndArgs ...any) bool {
		// 				return assert.ErrorIs(t, err, ErrVariableNameNotFound)
		// 			},
		// 		},
		{
			name: "反序列化成功_master字符串类型_无slave",
			yamlData: `
datasources:
  dbproxy_ds:
    master: webook:134root@tcp(cn.meoying.com:3306)/?charset=utf8mb4&parseTime=True`,
			varNames: []string{"dbproxy_ds"},
			getWantValue: func(t *testing.T, config *Config) []Datasource {
				t.Helper()
				return []Datasource{
					{
						varName: "dbproxy_ds",
						Master:  "webook:134root@tcp(cn.meoying.com:3306)/?charset=utf8mb4&parseTime=True",
						config:  config,
					},
				}
			},
			assertError: assert.NoError,
		},
		{
			name: "反序列化成功_master字符串类型_slave字符串类型",
			yamlData: `
datasources:
  dbproxy_ds:
    master: webook:134root@tcp(cn.meoying.com:3306)/?charset=utf8mb4&parseTime=True
    slave: webook:134root@tcp(cn.meoying.com:3306)/?charset=utf8mb4&parseTime=True`,
			varNames: []string{"dbproxy_ds"},
			getWantValue: func(t *testing.T, config *Config) []Datasource {
				t.Helper()
				return []Datasource{
					{
						varName: "dbproxy_ds",
						Master:  "webook:134root@tcp(cn.meoying.com:3306)/?charset=utf8mb4&parseTime=True",
						Slave:   String("webook:134root@tcp(cn.meoying.com:3306)/?charset=utf8mb4&parseTime=True"),
						config:  config,
					},
				}
			},
			assertError: assert.NoError,
		},
		{
			name: "反序列化成功_master字符串类型_slave模版类型",
			yamlData: `
datasources:
  dbproxy_ds:
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
			varNames: []string{"dbproxy_ds"},
			getWantValue: func(t *testing.T, config *Config) []Datasource {
				t.Helper()
				return []Datasource{
					{
						varName: "dbproxy_ds",
						Master:  "webook:134root@tcp(cn.meoying.com:3306)/?charset=utf8mb4&parseTime=True",
						Slave: any(Template{
							varName: "slave",
							Expr:    "webook:${password}@tcp(${region}.${id}.slave.meoying.com:3306)/?charset=utf8mb4&parseTime=True",
							Placeholders: map[string]any{
								"password": String("webook"),
								"region":   Enum{"cn", "hk"},
								"id":       Hash{varName: "id", Key: "user_id", Base: 3},
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
			name: "反序列化成功_slave为模版类型_占位符使用引用变量",
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
      master: webook:webook@tcp(us.meoying.com:3306)/?charset=utf8mb4&parseTime=True
      slave:
        template:
          expr: webook:${password}@tcp(${region}.${type}.slave.meoying.com:3306)/?charset=utf8mb4&parseTime=True
          placeholders:
            password:
              ref: variables.password
            region:
              ref: variables.region1
            type:
               ref: variables.index`,
			varNames: []string{"dbproxy_ds"},
			getWantValue: func(t *testing.T, config *Config) []Datasource {
				t.Helper()
				return []Datasource{
					{
						varName: "dbproxy_ds",
						Master:  "webook:webook@tcp(us.meoying.com:3306)/?charset=utf8mb4&parseTime=True",
						Slave: any(Template{
							varName: "slave",
							Expr:    "webook:${password}@tcp(${region}.${type}.slave.meoying.com:3306)/?charset=utf8mb4&parseTime=True",
							Placeholders: map[string]any{
								"password": String("webook"),
								"region":   Enum{"cn", "hk"},
								"type":     Hash{varName: "type", Key: "id", Base: 3},
							},
							config: config,
						}),
						config: config,
					},
				}
			},
			assertError: assert.NoError,
		},
		// 		{
		// 			name: "反序列化成功_多个_自引用",
		// 			yamlData: `
		// datasources:
		//   dbproxy_ds:
		//     - datasource:
		//         master: webook:134root@tcp(cn.meoying.com:3306)/?charset=utf8mb4&parseTime=True
		//         slave:
		//           template:
		//             expr: webook:webook@tcp(${region}.slave.meoying.com:3306)/?charset=utf8mb4&parseTime=True
		//             placeholders:
		//               region:
		//                 - cn
		//                 - hk
		//     - datasource:
		//         ref:
		//           - datasources.dbproxy_ds.0`,
		// 			dsName: "dbproxy_ds",
		// 			getWantValue: func(t *testing.T, config *Config) any {
		// 				t.Helper()
		// 				return []any{
		// 					&Datasource{
		// 						Master: "webook:134root@tcp(cn.meoying.com:3306)/?charset=utf8mb4&parseTime=True",
		// 						Slave: any(&Template{
		// 							Expr: "webook:webook@tcp(${region}.slave.meoying.com:3306)/?charset=utf8mb4&parseTime=True",
		// 							Placeholders: map[string]any{
		// 								"region": Enum{"cn", "hk"},
		// 							},
		// 							config: config,
		// 						}),
		// 						config: config,
		// 					},
		// 					&Datasource{
		// 						Ref: &Reference{
		// 							values: map[string]any{
		// 								"databases.dbproxy_ds.0": &Datasource{
		// 									Master: "webook:134root@tcp(cn.meoying.com:3306)/?charset=utf8mb4&parseTime=True",
		// 									Slave: any(&Template{
		// 										Expr: "webook:webook@tcp(${region}.slave.meoying.com:3306)/?charset=utf8mb4&parseTime=True",
		// 										Placeholders: map[string]any{
		// 											"region": Enum{"cn", "hk"},
		// 										},
		// 										config: config,
		// 									}),
		// 									config: config,
		// 								},
		// 							},
		// 							config: config,
		// 						},
		// 						config: config,
		// 					},
		// 				}
		// 			},
		// 			assertError: assert.NoError,
		// 		},
		{
			name: "反序列化成功_多个_不同变量之间引用_简单版",
			yamlData: `
datasources:
  webook_ds:
    ref: datasources.dbproxy_ds
  dbproxy_ds:
    master: webook:webook@tcp(us.meoying.com:3306)/?charset=utf8mb4&parseTime=True
    slave:
      template:
        expr: webook:${password}@tcp(slave.meoying.com:3306)/?charset=utf8mb4&parseTime=True
        placeholders:
          password: webook`,
			varNames: []string{"webook_ds"},
			getWantValue: func(t *testing.T, config *Config) []Datasource {
				t.Helper()
				return []Datasource{
					{
						varName: "webook_ds",
						Master:  "webook:webook@tcp(us.meoying.com:3306)/?charset=utf8mb4&parseTime=True",
						Slave: any(Template{
							varName: "slave",
							Expr:    "webook:${password}@tcp(slave.meoying.com:3306)/?charset=utf8mb4&parseTime=True",
							Placeholders: map[string]any{
								"password": String("webook"),
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
			name: "反序列化成功_多个_不同变量之间引用",
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
  webook_ds:
    ref: datasources.dbproxy_ds
  dbproxy_ds:
    master: webook:webook@tcp(us.meoying.com:3306)/?charset=utf8mb4&parseTime=True
    slave:
      template:
        expr: webook:${password}@tcp(${region}.${type}.slave.meoying.com:3306)/?charset=utf8mb4&parseTime=True
        placeholders:
          password:
            ref: variables.password
          region:
            ref: variables.region2
          type:
            ref: variables.index`,
			varNames: []string{"webook_ds"},
			getWantValue: func(t *testing.T, config *Config) []Datasource {
				t.Helper()
				return []Datasource{
					{
						varName: "webook_ds",
						Master:  "webook:webook@tcp(us.meoying.com:3306)/?charset=utf8mb4&parseTime=True",
						Slave: any(Template{
							varName: "slave",
							Expr:    "webook:${password}@tcp(${region}.${type}.slave.meoying.com:3306)/?charset=utf8mb4&parseTime=True",
							Placeholders: map[string]any{
								"password": String("webook"),
								"region":   Enum{"us", "uk"},
								"type":     Hash{varName: "type", Key: "id", Base: 3},
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

			assert.Subset(t, config.DatasourceNames(), tt.varNames)

			expectedValues := tt.getWantValue(t, &config)

			for i, varName := range tt.varNames {
				actual, err := config.DatasourceByName(varName)
				tt.assertError(t, err)
				if err != nil {
					return
				}
				assert.Equal(t, expectedValues[i], actual)
			}
		})
	}
}

func TestConfig_GetTableByName(t *testing.T) {

	tests := []struct {
		name         string
		yamlData     string
		varNames     []string
		getWantValue func(t *testing.T, config *Config) []Table
		assertError  assert.ErrorAssertionFunc
	}{
		{
			name: "基本类型",
			yamlData: `
tables:
  order:
    sharding:
      datasource:
        master: root:root@tcp(127.0.0.1:13306)/?charset=utf8mb4&parseTime=True&loc=Local
        slave: root:root@tcp(127.0.0.1:13306)/?charset=utf8mb4&parseTime=True&loc=Local
      database:
          template:
            expr: "local_sharding_plugin_db_${key}"
            placeholders:
              key:
                hash:
                  key: user_id
                  base: 3
      table: order_tab
`,
			varNames: []string{"order"},
			getWantValue: func(t *testing.T, config *Config) []Table {
				return []Table{
					{
						varName: "order",
						varType: DataTypeSharding,
						config:  config,
						Sharding: Sharding{
							varName: "order",
							config:  config,
							Datasource: Datasource{
								varName: "order",
								Master:  "root:root@tcp(127.0.0.1:13306)/?charset=utf8mb4&parseTime=True&loc=Local",
								Slave:   String("root:root@tcp(127.0.0.1:13306)/?charset=utf8mb4&parseTime=True&loc=Local"),
								config:  config,
							},
							Database: Database{
								varName: "order",
								varType: DataTypeTemplate,
								Value: any(Template{
									varName: "order",
									Expr:    "local_sharding_plugin_db_${key}",
									Placeholders: map[string]any{
										"key": Hash{varName: "key", Key: "user_id", Base: 3},
									},
									config: config,
								}),
								config: config,
							},
							Table: Variable{
								varName: "order",
								varType: DataTypeString,
								Value:   String("order_tab"),
								config:  config,
							},
						},
					},
				}
			},
			assertError: assert.NoError,
		},
		{
			name: "引用类型_引用datasources下变量",
			yamlData: `
datasources:
  order_ds:
    master: root:root@tcp(127.0.0.1:13306)/?charset=utf8mb4&parseTime=True&loc=Local
    slave: root:root@tcp(127.0.0.1:13306)/?charset=utf8mb4&parseTime=True&loc=Local
tables:
  order:
    sharding:
      datasource:
        ref: datasources.order_ds
      database:
        template:
          expr: "local_sharding_plugin_db_${key}"
          placeholders:
            key:
              hash:
                key: user_id
                base: 3
      table: order_tab
`,
			varNames: []string{"order"},
			getWantValue: func(t *testing.T, config *Config) []Table {
				return []Table{
					{
						varName: "order",
						varType: DataTypeSharding,
						config:  config,
						Sharding: Sharding{
							varName: "order",
							config:  config,
							Datasource: Datasource{
								varName: "datasources.order_ds",
								Master:  "root:root@tcp(127.0.0.1:13306)/?charset=utf8mb4&parseTime=True&loc=Local",
								Slave:   String("root:root@tcp(127.0.0.1:13306)/?charset=utf8mb4&parseTime=True&loc=Local"),
								config:  config,
							},
							Database: Database{
								varName: "order",
								varType: DataTypeTemplate,
								Value: any(Template{
									varName: "order",
									Expr:    "local_sharding_plugin_db_${key}",
									Placeholders: map[string]any{
										"key": Hash{varName: "key", Key: "user_id", Base: 3},
									},
									config: config,
								}),
								config: config,
							},
							Table: Variable{
								varName: "order",
								varType: DataTypeString,
								Value:   String("order_tab"),
								config:  config,
							},
						},
					},
				}
			},
			assertError: assert.NoError,
		},
		{
			name: "引用类型_引用databases下变量",
			yamlData: `
databases:
  order_db:
    template:
      expr: "local_sharding_plugin_db_${key}"
      placeholders:
        key:
          hash:
            key: user_id
            base: 3
tables:
  order:
    sharding:
      datasource:
        master: root:root@tcp(127.0.0.1:13306)/?charset=utf8mb4&parseTime=True&loc=Local
        slave: root:root@tcp(127.0.0.1:13306)/?charset=utf8mb4&parseTime=True&loc=Local
      database:
        ref: databases.order_db
      table: order_tab`,
			varNames: []string{"order"},
			getWantValue: func(t *testing.T, config *Config) []Table {
				return []Table{
					{
						varName: "order",
						varType: DataTypeSharding,
						config:  config,
						Sharding: Sharding{
							varName: "order",
							config:  config,
							Datasource: Datasource{
								varName: "order",
								Master:  "root:root@tcp(127.0.0.1:13306)/?charset=utf8mb4&parseTime=True&loc=Local",
								Slave:   String("root:root@tcp(127.0.0.1:13306)/?charset=utf8mb4&parseTime=True&loc=Local"),
								config:  config,
							},
							Database: Database{
								varName: "databases.order_db",
								varType: DataTypeTemplate,
								Value: any(Template{
									varName: "databases.order_db",
									Expr:    "local_sharding_plugin_db_${key}",
									Placeholders: map[string]any{
										"key": Hash{varName: "key", Key: "user_id", Base: 3},
									},
									config: config,
								}),
								config: config,
							},
							Table: Variable{
								varName: "order",
								varType: DataTypeString,
								Value:   String("order_tab"),
								config:  config,
							},
						},
					},
				}
			},
			assertError: assert.NoError,
		},
		{
			name: "引用类型_引用datasources和databases下变量",
			yamlData: `
datasources:
  order_ds:
    master: root:root@tcp(127.0.0.1:13306)/?charset=utf8mb4&parseTime=True&loc=Local
    slave: root:root@tcp(127.0.0.1:13306)/?charset=utf8mb4&parseTime=True&loc=Local
databases:
  order_db:
    template:
      expr: "local_sharding_plugin_db_${key}"
      placeholders:
        key:
          hash:
            key: user_id
            base: 3
tables:
  order:
    sharding:
      datasource:
        ref: datasources.order_ds
      database:
        ref: databases.order_db
      table: order_tab`,
			varNames: []string{"order"},
			getWantValue: func(t *testing.T, config *Config) []Table {
				return []Table{
					{
						varName: "order",
						varType: DataTypeSharding,
						config:  config,
						Sharding: Sharding{
							varName: "order",
							config:  config,
							Datasource: Datasource{
								varName: "datasources.order_ds",
								Master:  "root:root@tcp(127.0.0.1:13306)/?charset=utf8mb4&parseTime=True&loc=Local",
								Slave:   String("root:root@tcp(127.0.0.1:13306)/?charset=utf8mb4&parseTime=True&loc=Local"),
								config:  config,
							},
							Database: Database{
								varName: "databases.order_db",
								varType: DataTypeTemplate,
								Value: any(Template{
									varName: "databases.order_db",
									Expr:    "local_sharding_plugin_db_${key}",
									Placeholders: map[string]any{
										"key": Hash{varName: "key", Key: "user_id", Base: 3},
									},
									config: config,
								}),
								config: config,
							},
							Table: Variable{
								varName: "order",
								varType: DataTypeString,
								Value:   String("order_tab"),
								config:  config,
							},
						},
					},
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

			assert.Subset(t, config.TableNames(), tt.varNames)

			expectedValues := tt.getWantValue(t, &config)

			for i, varName := range tt.varNames {
				actual, err := config.TableByName(varName)
				tt.assertError(t, err)
				if err != nil {
					return
				}
				assert.Equal(t, expectedValues[i], actual)
			}

			// assert.Equal(t, tt.getWantValue(t, &config).(*Datasource).Slave.(*Template).Placeholders["password"], actual.(*Datasource).Slave.(*Template).Placeholders["password"])
			// assert.Equal(t, tt.getWantValue(t, &config).(*Datasource).Slave.(*Template).Placeholders["region"], actual.(*Datasource).Slave.(*Template).Placeholders["region"])
			// assert.Equal(t, tt.getWantValue(t, &config).(*Datasource).Slave.(*Template).Placeholders["type"], actual.(*Datasource).Slave.(*Template).Placeholders["type"])
		})
	}
}

func TestTableInfo(t *testing.T) {
	tests := []struct {
		name     string
		yamlData string
		varName  string

		wantMaser         string
		wantSlave         []string
		wantDatabaseNames []string
		assertError       assert.ErrorAssertionFunc
	}{
		{
			name: "基本类型",
			yamlData: `
tables:
  order:
    sharding:
      datasource:
        master: root:root@tcp(127.0.0.1:13306)/?charset=utf8mb4&parseTime=True&loc=Local
        slave: root:root@tcp(127.0.0.1:13306)/?charset=utf8mb4&parseTime=True&loc=Local
      database:
        template:
            expr: "local_sharding_plugin_db_${key}"
            placeholders:
              key:
                hash:
                  key: user_id
                  base: 3
      table: order_tab
`,
			varName:           "order",
			wantMaser:         "root:root@tcp(127.0.0.1:13306)/?charset=utf8mb4&parseTime=True&loc=Local",
			wantSlave:         []string{"root:root@tcp(127.0.0.1:13306)/?charset=utf8mb4&parseTime=True&loc=Local"},
			wantDatabaseNames: []string{"local_sharding_plugin_db_0", "local_sharding_plugin_db_1", "local_sharding_plugin_db_2"},
			assertError:       assert.NoError,
		},
		{
			name: "引用类型",
			yamlData: `
databases:
  order_db:
    template:
      expr: "local_sharding_plugin_db_${key}"
      placeholders:
        key:
          hash:
            key: user_id
            base: 3
tables:
  order:
    sharding:
      datasource:
        master: root:root@tcp(127.0.0.1:13306)/?charset=utf8mb4&parseTime=True&loc=Local
        slave: root:root@tcp(127.0.0.1:13306)/?charset=utf8mb4&parseTime=True&loc=Local
      database:
        ref: databases.order_db
      table: order_tab
`,
			varName:           "order",
			wantMaser:         "root:root@tcp(127.0.0.1:13306)/?charset=utf8mb4&parseTime=True&loc=Local",
			wantSlave:         []string{"root:root@tcp(127.0.0.1:13306)/?charset=utf8mb4&parseTime=True&loc=Local"},
			wantDatabaseNames: []string{"local_sharding_plugin_db_0", "local_sharding_plugin_db_1", "local_sharding_plugin_db_2"},
			assertError:       assert.NoError,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var config Config
			err := yaml.Unmarshal([]byte(tt.yamlData), &config)
			require.NoError(t, err)

			assert.Contains(t, config.TableNames(), tt.varName)

			actual, err := config.TableByName(tt.varName)
			tt.assertError(t, err)
			if err != nil {
				return
			}
			tb, ok := actual.(Table)
			require.True(t, ok)

			assert.Equal(t, tt.wantMaser, tb.MasterDSN())

			slave, err := tb.SlaveDSN()
			require.NoError(t, err)
			assert.Equal(t, tt.wantSlave, slave)

			names, err := tb.DatabaseNames()
			require.NoError(t, err)

			assert.Equal(t, tt.wantDatabaseNames, names)
		})
	}
}

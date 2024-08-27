package v2

import (
	"log"
	"testing"

	"github.com/stretchr/testify/assert"
	"gopkg.in/yaml.v3"
)

func TestRules(t *testing.T) {
	tests := []struct {
		name     string
		yamlData string

		want        Rules
		assertError assert.ErrorAssertionFunc
	}{
		// 局部定义datasources
		{
			name: "仅有datasources定义_多个命名主从",
			yamlData: `
rules:
  user:
    datasources:
      cn:
        master: webook:webook@tcp(cn.toB.mysql.meoying.com:3306)/order?xxx
        slaves:
          - webook:webook@tcp(0.cn.slave.toB.mysql.meoying.com:3306)/order?xxx
          - webook:webook@tcp(1.cn.slave.toB.mysql.meoying.com:3306)/order?xxx
      hk:
        master: webook:webook@tcp(hk.toB.mysql.meoying.com:3306)/order?xxx
        slaves:
          - webook:webook@tcp(2.hk.slave.toB.mysql.meoying.com:3306)/order?xxx
          - webook:webook@tcp(3.hk.slave.toB.mysql.meoying.com:3306)/order?xxx
`,
			want: Rules{
				Variables: map[string]Rule{
					"user": {
						Datasources: Section[Datasource]{
							Variables: map[string]Datasource{
								"cn": {
									Value: MasterSlaves{
										Master: "webook:webook@tcp(cn.toB.mysql.meoying.com:3306)/order?xxx",
										Slaves: Enum{
											"webook:webook@tcp(0.cn.slave.toB.mysql.meoying.com:3306)/order?xxx",
											"webook:webook@tcp(1.cn.slave.toB.mysql.meoying.com:3306)/order?xxx",
										},
									},
								},
								"hk": {
									Value: MasterSlaves{
										Master: "webook:webook@tcp(hk.toB.mysql.meoying.com:3306)/order?xxx",
										Slaves: Enum{
											"webook:webook@tcp(2.hk.slave.toB.mysql.meoying.com:3306)/order?xxx",
											"webook:webook@tcp(3.hk.slave.toB.mysql.meoying.com:3306)/order?xxx",
										},
									},
								},
							},
						},
					},
				},
			},
			assertError: assert.NoError,
		},
		{
			name: "仅有datasources定义_单个匿名主从",
			yamlData: `
rules:
  user:
    datasources:
        master: webook:webook@tcp(cn.toB.mysql.meoying.com:3306)/order?xxx
        slaves:
          - webook:webook@tcp(0.cn.slave.toB.mysql.meoying.com:3306)/order?xxx
          - webook:webook@tcp(1.cn.slave.toB.mysql.meoying.com:3306)/order?xxx
`,
			want: Rules{
				Variables: map[string]Rule{
					"user": {
						Datasources: Section[Datasource]{
							Variables: map[string]Datasource{
								"": {
									Value: MasterSlaves{
										Master: "webook:webook@tcp(cn.toB.mysql.meoying.com:3306)/order?xxx",
										Slaves: Enum{
											"webook:webook@tcp(0.cn.slave.toB.mysql.meoying.com:3306)/order?xxx",
											"webook:webook@tcp(1.cn.slave.toB.mysql.meoying.com:3306)/order?xxx",
										},
									},
								},
							},
						},
					},
				},
			},
			assertError: assert.NoError,
		},
		{
			name: "仅有datasources定义_应该报错_map形式多个匿名主从",
			yamlData: `
rules:
  user:
    datasources:
        master: webook:webook@tcp(cn.toB.mysql.meoying.com:3306)/order?xxx
        slaves:
          - webook:webook@tcp(0.cn.slave.toB.mysql.meoying.com:3306)/order?xxx
          - webook:webook@tcp(1.cn.slave.toB.mysql.meoying.com:3306)/order?xxx
        master: webook:webook@tcp(hk.toB.mysql.meoying.com:3306)/order?xxx
        slaves:
          - webook:webook@tcp(2.hk.slave.toB.mysql.meoying.com:3306)/order?xxx
          - webook:webook@tcp(3.hk.slave.toB.mysql.meoying.com:3306)/order?xxx
`,
			want: Rules{},
			assertError: func(t assert.TestingT, err error, i ...interface{}) bool {
				return assert.ErrorIs(t, err, ErrConfigSyntaxInvalid)
			},
		},
		{
			name: "仅有datasources定义_枚举形式多个匿名主从",
			yamlData: `
rules:
  user:
    datasources:
      - master: webook:webook@tcp(cn.toB.mysql.meoying.com:3306)/order?xxx
        slaves:
          - webook:webook@tcp(0.cn.slave.toB.mysql.meoying.com:3306)/order?xxx
          - webook:webook@tcp(1.cn.slave.toB.mysql.meoying.com:3306)/order?xxx
      - master: webook:webook@tcp(hk.toB.mysql.meoying.com:3306)/order?xxx
        slaves:
          - webook:webook@tcp(2.hk.slave.toB.mysql.meoying.com:3306)/order?xxx
          - webook:webook@tcp(3.hk.slave.toB.mysql.meoying.com:3306)/order?xxx
`,
			want: Rules{
				Variables: map[string]Rule{
					"user": {
						Datasources: Section[Datasource]{
							Variables: map[string]Datasource{
								"": {
									Value: []MasterSlaves{
										{
											Master: "webook:webook@tcp(cn.toB.mysql.meoying.com:3306)/order?xxx",
											Slaves: Enum{
												"webook:webook@tcp(0.cn.slave.toB.mysql.meoying.com:3306)/order?xxx",
												"webook:webook@tcp(1.cn.slave.toB.mysql.meoying.com:3306)/order?xxx",
											},
										},
										{
											Master: "webook:webook@tcp(hk.toB.mysql.meoying.com:3306)/order?xxx",
											Slaves: Enum{
												"webook:webook@tcp(2.hk.slave.toB.mysql.meoying.com:3306)/order?xxx",
												"webook:webook@tcp(3.hk.slave.toB.mysql.meoying.com:3306)/order?xxx",
											},
										},
									},
								},
							},
						},
					},
				},
			},
			assertError: assert.NoError,
		},
		{
			name: "仅有datasources定义_单个命名模版",
			yamlData: `
rules:
  user:
    datasources:
      named_tmpl:
        template:
          master: webook:webook@tcp(${region}.master.${role}.mysql.meoying.com:3306)/order?xxx
          slaves: webook:webook@tcp(${region}.slave.${role}.mysql.meoying.com:3306)/order?xxx
          placeholders:
            region:
              - hk
              - cn
            role:
              - test
              - prod
`,
			want: Rules{
				Variables: map[string]Rule{
					"user": {
						Datasources: Section[Datasource]{
							Variables: map[string]Datasource{
								"named_tmpl": {
									Value: DatasourceTemplate{
										Master: Template{
											Expr: "webook:webook@tcp(${region}.master.${role}.mysql.meoying.com:3306)/order?xxx",
											Placeholders: Section[Placeholder]{
												Variables: map[string]Placeholder{
													"region": {
														Value: Enum{"hk", "cn"},
													},
													"role": {
														Value: Enum{"test", "prod"},
													},
												},
											},
										},
										Slaves: Template{
											Expr: "webook:webook@tcp(${region}.slave.${role}.mysql.meoying.com:3306)/order?xxx",
											Placeholders: Section[Placeholder]{
												Variables: map[string]Placeholder{
													"region": {
														Value: Enum{"hk", "cn"},
													},
													"role": {
														Value: Enum{"test", "prod"},
													},
												},
											},
										},
									},
								},
							},
						},
					},
				},
			},
			assertError: assert.NoError,
		},
		{
			name: "仅有datasources定义_多个命名模版",
			yamlData: `
rules:
  user:
    datasources:
      named_tmpl:
        template:
          master: webook:webook@tcp(${region}.master.${role}.mysql.meoying.com:3306)/order?xxx
          slaves: webook:webook@tcp(${region}.slave.${role}.mysql.meoying.com:3306)/order?xxx
          placeholders:
            region:
              - hk
              - cn
            role:
              - test
              - prod
      named_tmpl2:
        template:
          master: webook:webook@tcp(${region}.master.${role}.mysql.meoying.com:3306)/order?xxx
          slaves: webook:webook@tcp(${region}.slave.${role}.mysql.meoying.com:3306)/order?xxx
          placeholders:
            region:
              - hk
              - cn
            role:
              - test
              - prod
`,
			want: Rules{
				Variables: map[string]Rule{
					"user": {
						Datasources: Section[Datasource]{
							Variables: map[string]Datasource{
								"named_tmpl": {
									Value: DatasourceTemplate{
										Master: Template{
											Expr: "webook:webook@tcp(${region}.master.${role}.mysql.meoying.com:3306)/order?xxx",
											Placeholders: Section[Placeholder]{
												Variables: map[string]Placeholder{
													"region": {
														Value: Enum{"hk", "cn"},
													},
													"role": {
														Value: Enum{"test", "prod"},
													},
												},
											},
										},
										Slaves: Template{
											Expr: "webook:webook@tcp(${region}.slave.${role}.mysql.meoying.com:3306)/order?xxx",
											Placeholders: Section[Placeholder]{
												Variables: map[string]Placeholder{
													"region": {
														Value: Enum{"hk", "cn"},
													},
													"role": {
														Value: Enum{"test", "prod"},
													},
												},
											},
										},
									},
								},
								"named_tmpl2": {
									Value: DatasourceTemplate{
										Master: Template{
											Expr: "webook:webook@tcp(${region}.master.${role}.mysql.meoying.com:3306)/order?xxx",
											Placeholders: Section[Placeholder]{
												Variables: map[string]Placeholder{
													"region": {
														Value: Enum{"hk", "cn"},
													},
													"role": {
														Value: Enum{"test", "prod"},
													},
												},
											},
										},
										Slaves: Template{
											Expr: "webook:webook@tcp(${region}.slave.${role}.mysql.meoying.com:3306)/order?xxx",
											Placeholders: Section[Placeholder]{
												Variables: map[string]Placeholder{
													"region": {
														Value: Enum{"hk", "cn"},
													},
													"role": {
														Value: Enum{"test", "prod"},
													},
												},
											},
										},
									},
								},
							},
						},
					},
				},
			},
			assertError: assert.NoError,
		},
		{
			name: "仅有datasources定义_单个匿名模版",
			yamlData: `
rules:
  user:
    datasources:
      template:
        master: webook:webook@tcp(${region}.master.${role}.mysql.meoying.com:3306)/order?xxx
        slaves: webook:webook@tcp(${region}.slave.${role}.mysql.meoying.com:3306)/order?xxx
        placeholders:
          region:
            - hk
            - cn
          role:
            - test
            - prod
`,
			want: Rules{
				Variables: map[string]Rule{
					"user": {
						Datasources: Section[Datasource]{
							Variables: map[string]Datasource{
								"ds_template": {
									Value: DatasourceTemplate{
										Master: Template{
											Expr: "webook:webook@tcp(${region}.master.${role}.mysql.meoying.com:3306)/order?xxx",
											Placeholders: Section[Placeholder]{
												Variables: map[string]Placeholder{
													"region": {
														Value: Enum{"hk", "cn"},
													},
													"role": {
														Value: Enum{"test", "prod"},
													},
												},
											},
										},
										Slaves: Template{
											Expr: "webook:webook@tcp(${region}.slave.${role}.mysql.meoying.com:3306)/order?xxx",
											Placeholders: Section[Placeholder]{
												Variables: map[string]Placeholder{
													"region": {
														Value: Enum{"hk", "cn"},
													},
													"role": {
														Value: Enum{"test", "prod"},
													},
												},
											},
										},
									},
								},
							},
						},
					},
				},
			},
			assertError: assert.NoError,
		},
		{
			name: "仅有datasources定义_应该报错_多个匿名模版",
			yamlData: `
rules:
  user:
    datasources:
      template:
        master: webook:webook@tcp(${region}.master.${role}.mysql.meoying.com:3306)/order?xxx
        slaves: webook:webook@tcp(${region}.slave.${role}.mysql.meoying.com:3306)/order?xxx
        placeholders:
          region:
            - hk
            - cn
          role:
            - test
            - prod
      template:
        master: webook:webook@tcp(${region}.master.${role}.mysql.meoying.com:3306)/order?xxx
        slaves: webook:webook@tcp(${region}.slave.${role}.mysql.meoying.com:3306)/order?xxx
        placeholders:
          region:
            - hk
            - cn
          role:
            - test
            - prod
`,
			want: Rules{},
			assertError: func(t assert.TestingT, err error, i ...interface{}) bool {
				log.Printf("err = %#v\n", err)
				return assert.ErrorIs(t, err, ErrConfigSyntaxInvalid)
			},
		},
		{
			name: "仅有datasources定义_应该报错_匿名模版与命名变量混用",
			yamlData: `
rules:
  user:
    datasources:
      cn:
        master: webook:webook@tcp(cn.toB.mysql.meoying.com:3306)/order?xxx
      template:
        master: webook:webook@tcp(${region}.master.${role}.mysql.meoying.com:3306)/order?xxx
        slaves: webook:webook@tcp(${region}.slave.${role}.mysql.meoying.com:3306)/order?xxx
        placeholders:
          region:
            - hk
            - cn
          role:
            - test
            - prod
`,
			want: Rules{},
			assertError: func(t assert.TestingT, err error, i ...interface{}) bool {
				return assert.ErrorIs(t, err, ErrUnmarshalVariableFailed)
			},
		},
		{
			name: "仅有datasources定义_引用多个全局变量",
			yamlData: `
datasources:
  cn_test:
    master: webook:webook@tcp(cn.master.test.mysql.meoying.com:3306)/order?xxx
    slaves:
      - webook:webook@tcp(cn.slave.test.mysql.meoying.com:3306)/order?xxx
  hk_test:
    master: webook:webook@tcp(hk.master.test.mysql.meoying.com:3306)/order?xxx
    slaves:
      - webook:webook@tcp(hk.slave.test.mysql.meoying.com:3306)/order?xxx
  cn_prod:
    master: webook:webook@tcp(cn.master.prod.mysql.meoying.com:3306)/order?xxx
    slaves:
      - webook:webook@tcp(cn.slave.prod.mysql.meoying.com:3306)/order?xxx
  hk_prod:
    master: webook:webook@tcp(hk.master.prod.mysql.meoying.com:3306)/order?xxx
    slaves:
      - webook:webook@tcp(hk.slave.prod.mysql.meoying.com:3306)/order?xxx
rules:
  user:
    datasources:
      ref:
        - datasources.hk_prod
        - datasources.hk_test
        - datasources.cn_test
        - datasources.cn_prod
`,
			want: Rules{
				Variables: map[string]Rule{
					"user": {
						Datasources: Section[Datasource]{
							Variables: map[string]Datasource{
								"cn_test": {
									Value: MasterSlaves{
										Master: "webook:webook@tcp(cn.master.test.mysql.meoying.com:3306)/order?xxx",
										Slaves: Enum{
											"webook:webook@tcp(cn.slave.test.mysql.meoying.com:3306)/order?xxx",
										},
									},
								},
								"hk_test": {
									Value: MasterSlaves{
										Master: "webook:webook@tcp(hk.master.test.mysql.meoying.com:3306)/order?xxx",
										Slaves: Enum{
											"webook:webook@tcp(hk.slave.test.mysql.meoying.com:3306)/order?xxx",
										},
									},
								},
								"cn_prod": {
									Value: MasterSlaves{
										Master: "webook:webook@tcp(cn.master.prod.mysql.meoying.com:3306)/order?xxx",
										Slaves: Enum{
											"webook:webook@tcp(cn.slave.prod.mysql.meoying.com:3306)/order?xxx",
										},
									},
								},
								"hk_prod": {
									Value: MasterSlaves{
										Master: "webook:webook@tcp(hk.master.prod.mysql.meoying.com:3306)/order?xxx",
										Slaves: Enum{
											"webook:webook@tcp(hk.slave.prod.mysql.meoying.com:3306)/order?xxx",
										},
									},
								},
							},
						},
					},
				},
			},
			assertError: assert.NoError,
		},
		{
			name: "仅有datasources定义_引用全局模版变量",
			yamlData: `
datasources:
  ds_tmpl:
    template:
      master: webook:webook@tcp(${region}.master.${role}.mysql.meoying.com:3306)/order?xxx
      slaves: webook:webook@tcp(${region}.slave.${role}.mysql.meoying.com:3306)/order?xxx
      placeholders:
        region:
          - hk
          - cn
        role:
          - test
          - prod
rules:
  user:
    datasources:
      ref:
        - datasources.ds_tmpl`,
			want: Rules{
				Variables: map[string]Rule{
					"user": {
						Datasources: Section[Datasource]{
							Variables: map[string]Datasource{
								"ds_tmpl": {
									Value: DatasourceTemplate{
										Master: Template{
											Expr: "webook:webook@tcp(${region}.master.${role}.mysql.meoying.com:3306)/order?xxx",
											Placeholders: Section[Placeholder]{
												Variables: map[string]Placeholder{
													"region": {
														Value: Enum{"hk", "cn"},
													},
													"role": {
														Value: Enum{"test", "prod"},
													},
												},
											},
										},
										Slaves: Template{
											Expr: "webook:webook@tcp(${region}.slave.${role}.mysql.meoying.com:3306)/order?xxx",
											Placeholders: Section[Placeholder]{
												Variables: map[string]Placeholder{
													"region": {
														Value: Enum{"hk", "cn"},
													},
													"role": {
														Value: Enum{"test", "prod"},
													},
												},
											},
										},
									},
								},
							},
						},
					},
				},
			},
			assertError: assert.NoError,
		},
		{
			name: "仅有datasources定义_引用路径错误",
			yamlData: `
datasources:
  cn_test:
    master: webook:webook@tcp(cn.master.test.mysql.meoying.com:3306)/order?xxx
    slaves:
      - webook:webook@tcp(cn.slave.test.mysql.meoying.com:3306)/order?xxx
  hk_test:
    master: webook:webook@tcp(hk.master.test.mysql.meoying.com:3306)/order?xxx
    slaves:
      - webook:webook@tcp(hk.slave.test.mysql.meoying.com:3306)/order?xxx
  cn_prod:
    master: webook:webook@tcp(cn.master.prod.mysql.meoying.com:3306)/order?xxx
    slaves:
      - webook:webook@tcp(cn.slave.prod.mysql.meoying.com:3306)/order?xxx
  hk_prod:
    master: webook:webook@tcp(hk.master.prod.mysql.meoying.com:3306)/order?xxx
    slaves:
      - webook:webook@tcp(hk.slave.prod.mysql.meoying.com:3306)/order?xxx
rules:
  user:
    datasources:
      ref:
        - abc.hk_prod
`,
			want: Rules{},
			assertError: func(t assert.TestingT, err error, i ...interface{}) bool {
				return assert.ErrorIs(t, err, ErrReferencePathInvalid)
			},
		},
		// 局部定义 databases
		{
			name: "仅有databases定义_匿名字符串",
			yamlData: `
rules:
  user:
    databases: user_db
`,
			want: Rules{
				Variables: map[string]Rule{
					"user": {
						Databases: Section[Database]{
							Variables: map[string]Database{
								"": {
									Value: String("user_db"),
								},
							},
						},
					},
				},
			},
			assertError: assert.NoError,
		},
		{
			name: "仅有databases定义_匿名引用字符串",
			yamlData: `
databases:
  user_db: user_db

rules:
  user:
    databases:
      ref:
        - databases.user_db
`,
			want: Rules{
				Variables: map[string]Rule{
					"user": {
						Databases: Section[Database]{
							Variables: map[string]Database{
								"user_db": {
									Value: String("user_db"),
								},
							},
						},
					},
				},
			},
			assertError: assert.NoError,
		},
		{
			name: "仅有databases定义_命名字符串",
			yamlData: `
rules:
  user:
    databases:
      cn: user_db
`,
			want: Rules{
				Variables: map[string]Rule{
					"user": {
						Databases: Section[Database]{
							Variables: map[string]Database{
								"cn": {
									Value: String("user_db"),
								},
							},
						},
					},
				},
			},
			assertError: assert.NoError,
		},
		{
			name: "仅有databases定义_命名引用字符串",
			yamlData: `
databases:
  user_db: user_db

rules:
  user:
    databases:
      cn:
        ref:
          - databases.user_db
`,
			want: Rules{
				Variables: map[string]Rule{
					"user": {
						Databases: Section[Database]{
							Variables: map[string]Database{
								"cn": {
									Value: String("user_db"),
								},
							},
						},
					},
				},
			},
			assertError: assert.NoError,
		},
		{
			name: "仅有databases定义_匿名枚举",
			yamlData: `
rules:
  user:
    databases:
      - user_db_0
      - user_db_1
`,
			want: Rules{
				Variables: map[string]Rule{
					"user": {
						Databases: Section[Database]{
							Variables: map[string]Database{
								"": {
									Value: Enum{"user_db_0", "user_db_1"},
								},
							},
						},
					},
				},
			},
			assertError: assert.NoError,
		},
		{
			name: "仅有databases定义_匿名引用枚举",
			yamlData: `
databases:
  user_dbs:
    - user_db_0
    - user_db_1

rules:
  user:
    databases:
      ref:
        - databases.user_dbs
`,
			want: Rules{
				Variables: map[string]Rule{
					"user": {
						Databases: Section[Database]{
							Variables: map[string]Database{
								"user_dbs": {
									Value: Enum{"user_db_0", "user_db_1"},
								},
							},
						},
					},
				},
			},
			assertError: assert.NoError,
		},
		{
			name: "仅有databases定义_命名枚举",
			yamlData: `
rules:
  user:
    databases:
      enum:
        - user_db_0
        - user_db_1
`,
			want: Rules{
				Variables: map[string]Rule{
					"user": {
						Databases: Section[Database]{
							Variables: map[string]Database{
								"enum": {
									Value: Enum{"user_db_0", "user_db_1"},
								},
							},
						},
					},
				},
			},
			assertError: assert.NoError,
		},
		{
			name: "仅有databases定义_命名引用枚举",
			yamlData: `
databases:
  user_dbs:
    - user_db_0
    - user_db_1

rules:
  user:
    databases:
      cn_user:
        ref:
          - databases.user_dbs
`,
			want: Rules{
				Variables: map[string]Rule{
					"user": {
						Databases: Section[Database]{
							Variables: map[string]Database{
								"cn_user": {
									Value: Enum{"user_db_0", "user_db_1"},
								},
							},
						},
					},
				},
			},
			assertError: assert.NoError,
		},
		{
			name: "仅有databases定义_匿名模版_直接定义占位符+引用全局占位符",
			yamlData: `
placeholders:
  id2: "1"
  region2:
    - us
    - uk
  index2:
    hash:
      key: user_id
      base: 10
rules:
  user:
    databases:
      template:
        expr: user_db_${ID}_${region}_${index}_${ID2}_${Region2}_${Index2}
        placeholders:
          ID: 0
          region:
            - cn
            - hk
          index:
            hash:
             key: user_id
             base: 3
          ID2:
            ref:
              - placeholders.id2
          Region2:
            ref:
              - placeholders.region2
          Index2:
            ref:
              - placeholders.index2
`,
			want: Rules{
				Variables: map[string]Rule{
					"user": {
						Databases: Section[Database]{
							Variables: map[string]Database{
								"template": {
									Value: Template{
										Expr: "user_db_${ID}_${region}_${index}_${ID2}_${Region2}_${Index2}",
										Placeholders: Section[Placeholder]{
											Variables: map[string]Placeholder{
												"ID": {
													Value: String("0"),
												},
												"region": {
													Value: Enum{"cn", "hk"},
												},
												"index": {
													Value: Hash{Key: "user_id", Base: 3},
												},
												"ID2": {
													Value: String("1"),
												},
												"Region2": {
													Value: Enum{"us", "uk"},
												},
												"Index2": {
													Value: Hash{Key: "user_id", Base: 10},
												},
											},
										},
									},
								},
							},
						},
					},
				},
			},
			assertError: assert.NoError,
		},
		{
			name: "仅有databases定义_匿名引用模版_被引用模版中包含引用全局占位符",
			yamlData: `
placeholders:
  id2: "1"
  region2:
    - us
    - uk
  index2:
    hash:
      key: user_id
      base: 10

databases:
  user:
    template:
        expr: user_db_${ID}_${region}_${index}_${ID2}_${Region2}_${Index2}
        placeholders:
          ID: 0
          region:
            - cn
            - hk
          index:
            hash:
             key: user_id
             base: 3
          ID2:
            ref:
              - placeholders.id2
          Region2:
            ref:
              - placeholders.region2
          Index2:
            ref:
              - placeholders.index2
rules:
  user:
    databases:
      ref:
        - databases.user
`,
			want: Rules{
				Variables: map[string]Rule{
					"user": {
						Databases: Section[Database]{
							Variables: map[string]Database{
								"user": {
									Value: Template{
										Expr: "user_db_${ID}_${region}_${index}_${ID2}_${Region2}_${Index2}",
										Placeholders: Section[Placeholder]{
											Variables: map[string]Placeholder{
												"ID": {
													Value: String("0"),
												},
												"region": {
													Value: Enum{"cn", "hk"},
												},
												"index": {
													Value: Hash{Key: "user_id", Base: 3},
												},
												"ID2": {
													Value: String("1"),
												},
												"Region2": {
													Value: Enum{"us", "uk"},
												},
												"Index2": {
													Value: Hash{Key: "user_id", Base: 10},
												},
											},
										},
									},
								},
							},
						},
					},
				},
			},
			assertError: assert.NoError,
		},
		{
			name: "仅有databases定义_命名模版_直接定义占位符+引用全局占位符",
			yamlData: `
placeholders:
  id2: "1"
  region2:
    - us
    - uk
  index2:
    hash:
      key: user_id
      base: 10
rules:
  user:
    databases:
      user_cn:
        template:
          expr: user_db_${ID}_${region}_${index}_${ID2}_${Region2}_${Index2}
          placeholders:
            ID: 0
            region:
              - cn
              - hk
            index:
              hash:
                key: user_id
                base: 3
            ID2:
              ref:
                - placeholders.id2
            Region2:
              ref:
                - placeholders.region2
            Index2:
              ref:
                - placeholders.index2
`,
			want: Rules{
				Variables: map[string]Rule{
					"user": {
						Databases: Section[Database]{
							Variables: map[string]Database{
								"user_cn": {
									Value: Template{
										Expr: "user_db_${ID}_${region}_${index}_${ID2}_${Region2}_${Index2}",
										Placeholders: Section[Placeholder]{
											Variables: map[string]Placeholder{
												"ID": {
													Value: String("0"),
												},
												"region": {
													Value: Enum{"cn", "hk"},
												},
												"index": {
													Value: Hash{Key: "user_id", Base: 3},
												},
												"ID2": {
													Value: String("1"),
												},
												"Region2": {
													Value: Enum{"us", "uk"},
												},
												"Index2": {
													Value: Hash{Key: "user_id", Base: 10},
												},
											},
										},
									},
								},
							},
						},
					},
				},
			},
			assertError: assert.NoError,
		},
		{
			name: "仅有databases定义_命名引用模版_被引用模版中包含引用全局占位符",
			yamlData: `
placeholders:
  id2: "1"
  region2:
    - us
    - uk
  index2:
    hash:
      key: user_id
      base: 10

databases:
  user:
    template:
        expr: user_db_${ID}_${region}_${index}_${ID2}_${Region2}_${Index2}
        placeholders:
          ID: 0
          region:
            - cn
            - hk
          index:
            hash:
             key: user_id
             base: 3
          ID2:
            ref:
              - placeholders.id2
          Region2:
            ref:
              - placeholders.region2
          Index2:
            ref:
              - placeholders.index2
rules:
  user:
    databases:
      cn:
        ref:
          - databases.user
      hk:
        ref:
          - databases.user
`,
			want: Rules{
				Variables: map[string]Rule{
					"user": {
						Databases: Section[Database]{
							Variables: map[string]Database{
								"cn": {
									Value: Template{
										Expr: "user_db_${ID}_${region}_${index}_${ID2}_${Region2}_${Index2}",
										Placeholders: Section[Placeholder]{
											Variables: map[string]Placeholder{
												"ID": {
													Value: String("0"),
												},
												"region": {
													Value: Enum{"cn", "hk"},
												},
												"index": {
													Value: Hash{Key: "user_id", Base: 3},
												},
												"ID2": {
													Value: String("1"),
												},
												"Region2": {
													Value: Enum{"us", "uk"},
												},
												"Index2": {
													Value: Hash{Key: "user_id", Base: 10},
												},
											},
										},
									},
								},
								"hk": {
									Value: Template{
										Expr: "user_db_${ID}_${region}_${index}_${ID2}_${Region2}_${Index2}",
										Placeholders: Section[Placeholder]{
											Variables: map[string]Placeholder{
												"ID": {
													Value: String("0"),
												},
												"region": {
													Value: Enum{"cn", "hk"},
												},
												"index": {
													Value: Hash{Key: "user_id", Base: 3},
												},
												"ID2": {
													Value: String("1"),
												},
												"Region2": {
													Value: Enum{"us", "uk"},
												},
												"Index2": {
													Value: Hash{Key: "user_id", Base: 10},
												},
											},
										},
									},
								},
							},
						},
					},
				},
			},
			assertError: assert.NoError,
		},
		{
			name: "仅有databases定义_模版语法_匿名与命名混合",
			yamlData: `
placeholders:
  id2: "1"
  region2:
    - us
    - uk
  index2:
    hash:
      key: user_id
      base: 10
rules:
  user:
    databases:
      user_cn:
        template:
          expr: user_db_${ID}_${region}_${index}_${ID2}_${Region2}_${Index2}
          placeholders:
            ID: 0
            region:
              - cn
              - hk
            index:
              hash:
                key: user_id
                base: 3
            ID2:
              ref:
                - placeholders.id2
            Region2:
              ref:
                - placeholders.region2
            Index2:
              ref:
                - placeholders.index2
      template:
        expr: user_db_${ID}_${region}_${index}_${ID2}_${Region2}_${Index2}
        placeholders:
          ID: 0
          region:
            - cn
            - hk
          index:
            hash:
             key: user_id
             base: 3
          ID2:
            ref:
              - placeholders.id2
          Region2:
            ref:
              - placeholders.region2
          Index2:
            ref:
              - placeholders.index2
`,
			want: Rules{
				Variables: map[string]Rule{
					"user": {
						Databases: Section[Database]{
							Variables: map[string]Database{
								"template": {
									Value: Template{
										Expr: "user_db_${ID}_${region}_${index}_${ID2}_${Region2}_${Index2}",
										Placeholders: Section[Placeholder]{
											Variables: map[string]Placeholder{
												"ID": {
													Value: String("0"),
												},
												"region": {
													Value: Enum{"cn", "hk"},
												},
												"index": {
													Value: Hash{Key: "user_id", Base: 3},
												},
												"ID2": {
													Value: String("1"),
												},
												"Region2": {
													Value: Enum{"us", "uk"},
												},
												"Index2": {
													Value: Hash{Key: "user_id", Base: 10},
												},
											},
										},
									},
								},
								"user_cn": {
									Value: Template{
										Expr: "user_db_${ID}_${region}_${index}_${ID2}_${Region2}_${Index2}",
										Placeholders: Section[Placeholder]{
											Variables: map[string]Placeholder{
												"ID": {
													Value: String("0"),
												},
												"region": {
													Value: Enum{"cn", "hk"},
												},
												"index": {
													Value: Hash{Key: "user_id", Base: 3},
												},
												"ID2": {
													Value: String("1"),
												},
												"Region2": {
													Value: Enum{"us", "uk"},
												},
												"Index2": {
													Value: Hash{Key: "user_id", Base: 10},
												},
											},
										},
									},
								},
							},
						},
					},
				},
			},
			assertError: assert.NoError,
		},
		// 局部定义 tables
		// 因与 databases 等价故省略对其的测试
		// 但当 tables 有不同于 datasources 的规则限制时需要添加
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var cfg Config
			cfg.testMode = true
			err := yaml.Unmarshal([]byte(tt.yamlData), &cfg)
			tt.assertError(t, err)
			if err != nil {
				return
			}
			assert.EqualExportedValues(t, tt.want, cfg.Rules, cfg.Rules)
		})
	}
}

func TestRules_UnmarshalError(t *testing.T) {

	tests := []struct {
		name     string
		yamlData string

		assertError assert.ErrorAssertionFunc
	}{
		{
			name: "缺少datasources",
			yamlData: `
rules:
  user:
    databases: user_db
    tables: user_tb
`,
			assertError: func(t assert.TestingT, err error, i ...interface{}) bool {
				log.Printf("err = %#v\n", err)
				return assert.Error(t, err, ErrConfigSyntaxInvalid)
			},
		},
		{
			name: "缺少databases",
			yamlData: `
rules:
  user:
    datasources: webook:webook@tcp(cn.master.meoying.com:3306)/?xxxx 
    tables: user_tb
`,
			assertError: func(t assert.TestingT, err error, i ...interface{}) bool {
				log.Printf("err = %#v\n", err)
				return assert.Error(t, err, ErrConfigSyntaxInvalid)
			},
		},
		{
			name: "缺少tables",
			yamlData: `
rules:
  user:
    datasources: webook:webook@tcp(cn.master.meoying.com:3306)/?xxxx 
    databases: user_db
`,
			assertError: func(t assert.TestingT, err error, i ...interface{}) bool {
				log.Printf("err = %#v\n", err)
				return assert.Error(t, err, ErrConfigSyntaxInvalid)
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var cfg Config
			err := yaml.Unmarshal([]byte(tt.yamlData), &cfg)
			tt.assertError(t, err)
		})
	}
}

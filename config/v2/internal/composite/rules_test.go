package composite

import (
	"testing"

	"github.com/meoying/dbproxy/config/v2/internal/errs"
	"github.com/stretchr/testify/assert"
	"gopkg.in/yaml.v3"
)

func TestRules(t *testing.T) {
	tests := []struct {
		name     string
		yamlData string

		getWantRules func(t *testing.T, ph *Placeholders, ds *Datasources, db *Databases, tb *Tables) Rules
		assertError  assert.ErrorAssertionFunc
	}{
		// 局部定义datasources
		{
			// TODO: 后续改为报错,必须包含数据源、数据库、数据表
			name: "仅有数据源定义_标准写法",
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
			getWantRules: func(t *testing.T, ph *Placeholders, ds *Datasources, db *Databases, tb *Tables) Rules {
				return Rules{
					placeholders: ph,
					datasources:  ds,
					Variables: map[string]Rule{
						"user": {
							globalPlaceholders: ph,
							globalDatasources:  ds,
							Datasources: Datasources{
								global:             ds,
								globalPlaceholders: ph,
								Variables: map[string]Datasource{
									"cn": {
										MasterSlaves: MasterSlaves{
											Master: "webook:webook@tcp(cn.toB.mysql.meoying.com:3306)/order?xxx",
											Slaves: Enum{
												"webook:webook@tcp(0.cn.slave.toB.mysql.meoying.com:3306)/order?xxx",
												"webook:webook@tcp(1.cn.slave.toB.mysql.meoying.com:3306)/order?xxx",
											},
										},
									},
									"hk": {
										MasterSlaves: MasterSlaves{
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
				}
			},
			assertError: assert.NoError,
		},
		{
			// TODO: 后续改为报错,必须包含数据源、数据库、数据表
			name: "仅有局部数据源定义_应该报错_模版语法_匿名",
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
			getWantRules: func(t *testing.T, ph *Placeholders, ds *Datasources, db *Databases, tb *Tables) Rules {
				return Rules{}
			},
			assertError: func(t assert.TestingT, err error, i ...interface{}) bool {
				return assert.ErrorIs(t, err, errs.ErrUnmarshalVariableFailed)
			},
		},
		{
			// TODO: 后续改为报错,必须包含数据源、数据库、数据表
			name: "仅有局部数据源定义_应该报错_模版语法_匿名与命名混合",
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
			getWantRules: func(t *testing.T, ph *Placeholders, ds *Datasources, db *Databases, tb *Tables) Rules {
				return Rules{}
			},
			assertError: func(t assert.TestingT, err error, i ...interface{}) bool {
				return assert.ErrorIs(t, err, errs.ErrUnmarshalVariableFailed)
			},
		},
		{
			// TODO: 后续改为报错,必须包含数据源、数据库、数据表
			name: "仅有局部数据源定义_模版语法_命名",
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
			getWantRules: func(t *testing.T, ph *Placeholders, ds *Datasources, db *Databases, tb *Tables) Rules {
				return Rules{
					placeholders: ph,
					datasources:  ds,
					Variables: map[string]Rule{
						"user": {
							globalPlaceholders: ph,
							globalDatasources:  ds,
							Datasources: Datasources{
								globalPlaceholders: ph,
								global:             ds,
								Variables: map[string]Datasource{
									"named_tmpl": {
										Template: DatasourceTemplate{
											global: ph,
											Master: Template{
												global: ph,
												Expr:   "webook:webook@tcp(${region}.master.${role}.mysql.meoying.com:3306)/order?xxx",
												Placeholders: Placeholders{
													global: ph,
													variables: map[string]Placeholder{
														"region": {
															Enum: Enum{"hk", "cn"},
														},
														"role": {
															Enum: Enum{"test", "prod"},
														},
													},
												},
											},
											Slaves: Template{
												global: ph,
												Expr:   "webook:webook@tcp(${region}.slave.${role}.mysql.meoying.com:3306)/order?xxx",
												Placeholders: Placeholders{
													global: ph,
													variables: map[string]Placeholder{
														"region": {
															Enum: Enum{"hk", "cn"},
														},
														"role": {
															Enum: Enum{"test", "prod"},
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
				}
			},
			assertError: assert.NoError,
		},
		{
			// TODO: 后续改为报错,必须包含数据源、数据库、数据表
			name: "仅有局部数据源定义_模版语法_命名_多个",
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
			getWantRules: func(t *testing.T, ph *Placeholders, ds *Datasources, db *Databases, tb *Tables) Rules {
				return Rules{
					placeholders: ph,
					datasources:  ds,
					Variables: map[string]Rule{
						"user": {
							globalPlaceholders: ph,
							globalDatasources:  ds,
							Datasources: Datasources{
								global:             ds,
								globalPlaceholders: ph,
								Variables: map[string]Datasource{
									"named_tmpl": {
										Template: DatasourceTemplate{
											global: ph,
											Master: Template{
												global: ph,
												Expr:   "webook:webook@tcp(${region}.master.${role}.mysql.meoying.com:3306)/order?xxx",
												Placeholders: Placeholders{
													global: ph,
													variables: map[string]Placeholder{
														"region": {
															Enum: Enum{"hk", "cn"},
														},
														"role": {
															Enum: Enum{"test", "prod"},
														},
													},
												},
											},
											Slaves: Template{
												global: ph,
												Expr:   "webook:webook@tcp(${region}.slave.${role}.mysql.meoying.com:3306)/order?xxx",
												Placeholders: Placeholders{
													global: ph,
													variables: map[string]Placeholder{
														"region": {
															Enum: Enum{"hk", "cn"},
														},
														"role": {
															Enum: Enum{"test", "prod"},
														},
													},
												},
											},
										},
									},
									"named_tmpl2": {
										Template: DatasourceTemplate{
											global: ph,
											Master: Template{
												global: ph,
												Expr:   "webook:webook@tcp(${region}.master.${role}.mysql.meoying.com:3306)/order?xxx",
												Placeholders: Placeholders{
													global: ph,
													variables: map[string]Placeholder{
														"region": {
															Enum: Enum{"hk", "cn"},
														},
														"role": {
															Enum: Enum{"test", "prod"},
														},
													},
												},
											},
											Slaves: Template{
												global: ph,
												Expr:   "webook:webook@tcp(${region}.slave.${role}.mysql.meoying.com:3306)/order?xxx",
												Placeholders: Placeholders{
													global: ph,
													variables: map[string]Placeholder{
														"region": {
															Enum: Enum{"hk", "cn"},
														},
														"role": {
															Enum: Enum{"test", "prod"},
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
				}
			},
			assertError: assert.NoError,
		},
		// TODO: 局部定义 placeholders —— 在 datasources 中 引用全局 placeholders
		{
			// TODO: 后续改为报错,必须包含数据源、数据库、数据表
			name: "仅有局部数据源定义_引用语法_数据源类型",
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
			getWantRules: func(t *testing.T, ph *Placeholders, ds *Datasources, db *Databases, tb *Tables) Rules {
				return Rules{
					placeholders: ph,
					datasources:  ds,
					Variables: map[string]Rule{
						"user": {
							globalPlaceholders: ph,
							globalDatasources:  ds,
							Datasources: Datasources{
								global:             ds,
								globalPlaceholders: ph,
								Variables: map[string]Datasource{
									"cn_test": {
										MasterSlaves: MasterSlaves{
											Master: "webook:webook@tcp(cn.master.test.mysql.meoying.com:3306)/order?xxx",
											Slaves: Enum{
												"webook:webook@tcp(cn.slave.test.mysql.meoying.com:3306)/order?xxx",
											},
										},
									},
									"hk_test": {
										MasterSlaves: MasterSlaves{
											Master: "webook:webook@tcp(hk.master.test.mysql.meoying.com:3306)/order?xxx",
											Slaves: Enum{
												"webook:webook@tcp(hk.slave.test.mysql.meoying.com:3306)/order?xxx",
											},
										},
									},
									"cn_prod": {
										MasterSlaves: MasterSlaves{
											Master: "webook:webook@tcp(cn.master.prod.mysql.meoying.com:3306)/order?xxx",
											Slaves: Enum{
												"webook:webook@tcp(cn.slave.prod.mysql.meoying.com:3306)/order?xxx",
											},
										},
									},
									"hk_prod": {
										MasterSlaves: MasterSlaves{
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
				}
			},
			assertError: assert.NoError,
		},
		{
			// TODO: 后续改为报错,必须包含数据源、数据库、数据表
			name: "仅有局部数据源定义_引用语法_模版类型",
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
			getWantRules: func(t *testing.T, ph *Placeholders, ds *Datasources, db *Databases, tb *Tables) Rules {
				return Rules{
					datasources:  ds,
					placeholders: ph,
					Variables: map[string]Rule{
						"user": {
							globalDatasources:  ds,
							globalPlaceholders: ph,
							Datasources: Datasources{
								global:             ds,
								globalPlaceholders: ph,
								Variables: map[string]Datasource{
									"ds_tmpl": {
										Template: DatasourceTemplate{
											global: ph,
											Master: Template{
												global: ph,
												Expr:   "webook:webook@tcp(${region}.master.${role}.mysql.meoying.com:3306)/order?xxx",
												Placeholders: Placeholders{
													global: ph,
													variables: map[string]Placeholder{
														"region": {
															Enum: Enum{"hk", "cn"},
														},
														"role": {
															Enum: Enum{"test", "prod"},
														},
													},
												},
											},
											Slaves: Template{
												global: ph,
												Expr:   "webook:webook@tcp(${region}.slave.${role}.mysql.meoying.com:3306)/order?xxx",
												Placeholders: Placeholders{
													global: ph,
													variables: map[string]Placeholder{
														"region": {
															Enum: Enum{"hk", "cn"},
														},
														"role": {
															Enum: Enum{"test", "prod"},
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
				}
			},
			assertError: assert.NoError,
		},
		{
			// TODO: 后续改为报错,必须包含数据源、数据库、数据表
			name: "仅有数据源定义_引用路径错误",
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
			getWantRules: func(t *testing.T, ph *Placeholders, ds *Datasources, db *Databases, tb *Tables) Rules {
				return Rules{}
			},
			assertError: func(t assert.TestingT, err error, i ...interface{}) bool {
				return assert.ErrorIs(t, err, errs.ErrReferencePathInvalid)
			},
		},

		// 局部定义databases
		{
			// TODO: 后续改为报错,必须包含数据源、数据库、数据表
			name: "仅有databases定义_字符串",
			yamlData: `
rules:
  user:
    databases: user_db
`,
			getWantRules: func(t *testing.T, ph *Placeholders, ds *Datasources, db *Databases, tb *Tables) Rules {
				return Rules{
					placeholders: ph,
					datasources:  ds,
					databases:    db,
					Variables: map[string]Rule{
						"user": {
							globalPlaceholders: ph,
							globalDatasources:  ds,
							globalDatabases:    db,
							Databases: Databases{
								global:             db,
								globalPlaceholders: ph,
								Variables: map[string]Database{
									"": {
										Value: String("user_db"),
									},
								},
							},
						},
					},
				}
			},
			assertError: assert.NoError,
		},
		{
			// TODO: 后续改为报错,必须包含数据源、数据库、数据表
			name: "仅有databases定义_引用字符串",
			yamlData: `
databases:
  user_db: user_db

rules:
  user:
    databases:
      ref:
        - databases.user_db
`,
			getWantRules: func(t *testing.T, ph *Placeholders, ds *Datasources, db *Databases, tb *Tables) Rules {
				return Rules{
					placeholders: ph,
					datasources:  ds,
					databases:    db,
					Variables: map[string]Rule{
						"user": {
							globalPlaceholders: ph,
							globalDatasources:  ds,
							globalDatabases:    db,
							Databases: Databases{
								global:             db,
								globalPlaceholders: ph,
								Variables: map[string]Database{
									"user_db": {
										Value: String("user_db"),
									},
								},
							},
						},
					},
				}
			},
			assertError: assert.NoError,
		},
		{
			// TODO: 后续改为报错,必须包含数据源、数据库、数据表
			name: "仅有databases定义_枚举",
			yamlData: `
rules:
  user:
    databases:
      - user_db_0
      - user_db_1
`,
			getWantRules: func(t *testing.T, ph *Placeholders, ds *Datasources, db *Databases, tb *Tables) Rules {
				return Rules{
					placeholders: ph,
					datasources:  ds,
					databases:    db,
					Variables: map[string]Rule{
						"user": {
							globalPlaceholders: ph,
							globalDatasources:  ds,
							globalDatabases:    db,
							Databases: Databases{
								global:             db,
								globalPlaceholders: ph,
								Variables: map[string]Database{
									"": {
										Value: Enum{"user_db_0", "user_db_1"},
									},
								},
							},
						},
					},
				}
			},
			assertError: assert.NoError,
		},
		// 引用枚举

		// 模版语法
		// 引用模版

		// 模版语法_引用占位符
		// TODO: 局部定义 placeholders —— 在 databases 中 引用全局 placeholders

		// 局部定义tables
		{
			// TODO: 后续改为报错,必须包含数据源、数据库、数据表
			name: "仅有tables定义_字符串",
			yamlData: `
rules:
  user:
    tables: user_tbl
`,
			getWantRules: func(t *testing.T, ph *Placeholders, ds *Datasources, db *Databases, tb *Tables) Rules {
				return Rules{
					placeholders: ph,
					datasources:  ds,
					databases:    db,
					Variables: map[string]Rule{
						"user": {
							globalPlaceholders: ph,
							globalDatasources:  ds,
							globalDatabases:    db,
							Tables: Tables{
								global:             tb,
								globalPlaceholders: ph,
								Variables: map[string]Table{
									"": {
										Value: String("user_tbl"),
									},
								},
							},
						},
					},
				}
			},
			assertError: assert.NoError,
		},
		{
			// TODO: 后续改为报错,必须包含数据源、数据库、数据表
			name: "仅有tables定义_引用字符串",
			yamlData: `
tables:
  user_table: user_table

rules:
  user:
    tables:
      ref:
        - tables.user_table
`,
			getWantRules: func(t *testing.T, ph *Placeholders, ds *Datasources, db *Databases, tb *Tables) Rules {
				return Rules{
					placeholders: ph,
					datasources:  ds,
					databases:    db,
					Variables: map[string]Rule{
						"user": {
							globalPlaceholders: ph,
							globalDatasources:  ds,
							globalDatabases:    db,
							Tables: Tables{
								global:             tb,
								globalPlaceholders: ph,
								Variables: map[string]Table{
									"user_table": {
										Value: String("user_table"),
									},
								},
							},
						},
					},
				}
			},
			assertError: assert.NoError,
		},
		{
			// TODO: 后续改为报错,必须包含数据源、数据库、数据表
			name: "仅有tables定义_枚举",
			yamlData: `
rules:
  user:
    tables:
      - user_db_0
      - user_db_1
`,
			getWantRules: func(t *testing.T, ph *Placeholders, ds *Datasources, db *Databases, tb *Tables) Rules {
				return Rules{
					placeholders: ph,
					datasources:  ds,
					databases:    db,
					Variables: map[string]Rule{
						"user": {
							globalPlaceholders: ph,
							globalDatasources:  ds,
							globalDatabases:    db,
							Tables: Tables{
								global:             tb,
								globalPlaceholders: ph,
								Variables: map[string]Table{
									"": {
										Value: Enum{"user_db_0", "user_db_1"},
									},
								},
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
			var cfg Config
			cfg.testMode = true
			err := yaml.Unmarshal([]byte(tt.yamlData), &cfg)
			tt.assertError(t, err)
			if err != nil {
				return
			}
			assert.EqualExportedValues(t, tt.getWantRules(t, cfg.Placeholders, cfg.Datasources, cfg.Databases, nil), cfg.Rules)
		})
	}
}

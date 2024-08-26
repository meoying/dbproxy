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

		getWantRules func(t *testing.T, ph *Section[Placeholder], ds *Datasources, db *Section[Database], tb *Section[Table]) Rules
		assertError  assert.ErrorAssertionFunc
	}{
		// 局部定义datasources
		{
			// TODO: 后续改为报错,必须包含数据源、数据库、数据表
			name: "仅有datasources定义_标准写法",
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
			getWantRules: func(t *testing.T, ph *Section[Placeholder], ds *Datasources, db *Section[Database], tb *Section[Table]) Rules {
				return Rules{
					Variables: map[string]Rule{
						"user": {
							Datasources: Datasources{
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
			name: "仅有datasources定义_应该报错_模版语法_匿名",
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
			getWantRules: func(t *testing.T, ph *Section[Placeholder], ds *Datasources, db *Section[Database], tb *Section[Table]) Rules {
				return Rules{}
			},
			assertError: func(t assert.TestingT, err error, i ...interface{}) bool {
				return assert.ErrorIs(t, err, errs.ErrUnmarshalVariableFailed)
			},
		},
		{
			// TODO: 后续改为报错,必须包含数据源、数据库、数据表
			name: "仅有datasources定义_应该报错_模版语法_匿名与命名混合",
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
			getWantRules: func(t *testing.T, ph *Section[Placeholder], ds *Datasources, db *Section[Database], tb *Section[Table]) Rules {
				return Rules{}
			},
			assertError: func(t assert.TestingT, err error, i ...interface{}) bool {
				return assert.ErrorIs(t, err, errs.ErrUnmarshalVariableFailed)
			},
		},
		{
			// TODO: 后续改为报错,必须包含数据源、数据库、数据表
			name: "仅有datasources定义_模版语法_命名",
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
			getWantRules: func(t *testing.T, ph *Section[Placeholder], ds *Datasources, db *Section[Database], tb *Section[Table]) Rules {
				return Rules{
					Variables: map[string]Rule{
						"user": {
							Datasources: Datasources{
								Variables: map[string]Datasource{
									"named_tmpl": {
										Template: DatasourceTemplate{
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
				}
			},
			assertError: assert.NoError,
		},
		{
			// TODO: 后续改为报错,必须包含数据源、数据库、数据表
			name: "仅有datasources定义_模版语法_命名_多个",
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
			getWantRules: func(t *testing.T, ph *Section[Placeholder], ds *Datasources, db *Section[Database], tb *Section[Table]) Rules {
				return Rules{
					Variables: map[string]Rule{
						"user": {
							Datasources: Datasources{
								Variables: map[string]Datasource{
									"named_tmpl": {
										Template: DatasourceTemplate{
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
										Template: DatasourceTemplate{
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
				}
			},
			assertError: assert.NoError,
		},
		// TODO: 局部定义 placeholders —— 在 datasources 中 引用全局 placeholders
		{
			// TODO: 后续改为报错,必须包含数据源、数据库、数据表
			name: "仅有datasources定义_引用语法_数据源类型",
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
			getWantRules: func(t *testing.T, ph *Section[Placeholder], ds *Datasources, db *Section[Database], tb *Section[Table]) Rules {
				return Rules{
					Variables: map[string]Rule{
						"user": {
							Datasources: Datasources{
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
			name: "仅有datasources定义_引用语法_模版类型",
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
			getWantRules: func(t *testing.T, ph *Section[Placeholder], ds *Datasources, db *Section[Database], tb *Section[Table]) Rules {
				return Rules{
					Variables: map[string]Rule{
						"user": {
							Datasources: Datasources{
								Variables: map[string]Datasource{
									"ds_tmpl": {
										Template: DatasourceTemplate{
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
				}
			},
			assertError: assert.NoError,
		},
		{
			// TODO: 后续改为报错,必须包含数据源、数据库、数据表
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
			getWantRules: func(t *testing.T, ph *Section[Placeholder], ds *Datasources, db *Section[Database], tb *Section[Table]) Rules {
				return Rules{}
			},
			assertError: func(t assert.TestingT, err error, i ...interface{}) bool {
				return assert.ErrorIs(t, err, errs.ErrReferencePathInvalid)
			},
		},

		// 局部定义databases
		{
			// TODO: 后续改为报错,必须包含数据源、数据库、数据表
			name: "仅有databases定义_匿名字符串",
			yamlData: `
rules:
  user:
    databases: user_db
`,
			getWantRules: func(t *testing.T, ph *Section[Placeholder], ds *Datasources, db *Section[Database], tb *Section[Table]) Rules {
				return Rules{
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
				}
			},
			assertError: assert.NoError,
		},
		{
			// TODO: 后续改为报错,必须包含数据源、数据库、数据表
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
			getWantRules: func(t *testing.T, ph *Section[Placeholder], ds *Datasources, db *Section[Database], tb *Section[Table]) Rules {
				return Rules{
					Variables: map[string]Rule{
						"user": {
							Databases: Section[Database]{
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
			name: "仅有databases定义_命名字符串",
			yamlData: `
rules:
  user:
    databases:
      cn: user_db
`,
			getWantRules: func(t *testing.T, ph *Section[Placeholder], ds *Datasources, db *Section[Database], tb *Section[Table]) Rules {
				return Rules{
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
				}
			},
			assertError: assert.NoError,
		},
		{
			// TODO: 后续改为报错,必须包含数据源、数据库、数据表
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
			getWantRules: func(t *testing.T, ph *Section[Placeholder], ds *Datasources, db *Section[Database], tb *Section[Table]) Rules {
				return Rules{
					Variables: map[string]Rule{
						"user": {
							Databases: Section[Database]{
								global:             db,
								globalPlaceholders: ph,
								Variables: map[string]Database{
									"cn": {
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
			name: "仅有databases定义_匿名枚举",
			yamlData: `
rules:
  user:
    databases:
      - user_db_0
      - user_db_1
`,
			getWantRules: func(t *testing.T, ph *Section[Placeholder], ds *Datasources, db *Section[Database], tb *Section[Table]) Rules {
				return Rules{
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
				}
			},
			assertError: assert.NoError,
		},
		{
			// TODO: 后续改为报错,必须包含数据源、数据库、数据表
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
			getWantRules: func(t *testing.T, ph *Section[Placeholder], ds *Datasources, db *Section[Database], tb *Section[Table]) Rules {
				return Rules{
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
				}
			},
			assertError: assert.NoError,
		},
		{
			// TODO: 后续改为报错,必须包含数据源、数据库、数据表
			name: "仅有databases定义_命名枚举",
			yamlData: `
rules:
  user:
    databases:
      enum:
        - user_db_0
        - user_db_1
`,
			getWantRules: func(t *testing.T, ph *Section[Placeholder], ds *Datasources, db *Section[Database], tb *Section[Table]) Rules {
				return Rules{
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
				}
			},
			assertError: assert.NoError,
		},
		{
			// TODO: 后续改为报错,必须包含数据源、数据库、数据表
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
			getWantRules: func(t *testing.T, ph *Section[Placeholder], ds *Datasources, db *Section[Database], tb *Section[Table]) Rules {
				return Rules{
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
				}
			},
			assertError: assert.NoError,
		},
		{
			// TODO: 后续改为报错,必须包含数据源、数据库、数据表
			name: "仅有databases定义_匿名模版",
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
			getWantRules: func(t *testing.T, ph *Section[Placeholder], ds *Datasources, db *Section[Database], tb *Section[Table]) Rules {
				return Rules{
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
				}
			},
			assertError: assert.NoError,
		},
		{
			// TODO: 后续改为报错,必须包含数据源、数据库、数据表
			name: "仅有databases定义_匿名引用模版",
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
			getWantRules: func(t *testing.T, ph *Section[Placeholder], ds *Datasources, db *Section[Database], tb *Section[Table]) Rules {
				return Rules{
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
				}
			},
			assertError: assert.NoError,
		},
		{
			// TODO: 后续改为报错,必须包含数据源、数据库、数据表
			name: "仅有databases定义_命名模版",
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
			getWantRules: func(t *testing.T, ph *Section[Placeholder], ds *Datasources, db *Section[Database], tb *Section[Table]) Rules {
				return Rules{
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
				}
			},
			assertError: assert.NoError,
		},
		{
			// TODO: 后续改为报错,必须包含数据源、数据库、数据表
			name: "仅有databases定义_命名引用模版",
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
			getWantRules: func(t *testing.T, ph *Section[Placeholder], ds *Datasources, db *Section[Database], tb *Section[Table]) Rules {
				return Rules{
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
				}
			},
			assertError: assert.NoError,
		},
		{
			// TODO: 后续改为报错,必须包含数据源、数据库、数据表
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
			getWantRules: func(t *testing.T, ph *Section[Placeholder], ds *Datasources, db *Section[Database], tb *Section[Table]) Rules {
				return Rules{
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
				}
			},
			assertError: assert.NoError,
		},
		// 局部定义tables
		{
			// TODO: 后续改为报错,必须包含数据源、数据库、数据表
			name: "仅有tables定义_字符串",
			yamlData: `
rules:
  user:
    tables: user_tbl
`,
			getWantRules: func(t *testing.T, ph *Section[Placeholder], ds *Datasources, db *Section[Database], tb *Section[Table]) Rules {
				return Rules{
					Variables: map[string]Rule{
						"user": {
							Tables: Section[Table]{
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
			getWantRules: func(t *testing.T, ph *Section[Placeholder], ds *Datasources, db *Section[Database], tb *Section[Table]) Rules {
				return Rules{
					Variables: map[string]Rule{
						"user": {
							Tables: Section[Table]{
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
      - user_tb_0
      - user_tb_1
`,
			getWantRules: func(t *testing.T, ph *Section[Placeholder], ds *Datasources, db *Section[Database], tb *Section[Table]) Rules {
				return Rules{
					Variables: map[string]Rule{
						"user": {
							Tables: Section[Table]{
								global:             tb,
								globalPlaceholders: ph,
								Variables: map[string]Table{
									"": {
										Value: Enum{"user_tb_0", "user_tb_1"},
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
			name: "仅有tables定义_引用枚举",
			yamlData: `
tables:
  user_tables:
    - user_tb_0
    - user_tb_1

rules:
  user:
    tables:
      ref:
        - tables.user_tables
`,
			getWantRules: func(t *testing.T, ph *Section[Placeholder], ds *Datasources, db *Section[Database], tb *Section[Table]) Rules {
				return Rules{
					Variables: map[string]Rule{
						"user": {
							Tables: Section[Table]{
								Variables: map[string]Table{
									"user_tables": {
										Value: Enum{"user_tb_0", "user_tb_1"},
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
			name: "仅有tables定义_模版语法",
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
    tables:
      template:
        expr: user_tb_${ID}_${region}_${index}_${ID2}_${Region2}_${Index2}
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
			getWantRules: func(t *testing.T, ph *Section[Placeholder], ds *Datasources, db *Section[Database], tb *Section[Table]) Rules {
				return Rules{
					Variables: map[string]Rule{
						"user": {
							Tables: Section[Table]{
								Variables: map[string]Table{
									"template": {
										Value: Template{
											Expr: "user_tb_${ID}_${region}_${index}_${ID2}_${Region2}_${Index2}",
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
				}
			},
			assertError: assert.NoError,
		},
		{
			// TODO: 后续改为报错,必须包含数据源、数据库、数据表
			name: "仅有tables定义_引用模版语法",
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

tables:
  user:
    template:
        expr: user_tb_${ID}_${region}_${index}_${ID2}_${Region2}_${Index2}
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
    tables:
      ref:
        - tables.user
`,
			getWantRules: func(t *testing.T, ph *Section[Placeholder], ds *Datasources, db *Section[Database], tb *Section[Table]) Rules {
				return Rules{
					Variables: map[string]Rule{
						"user": {
							Tables: Section[Table]{
								Variables: map[string]Table{
									"user": {
										Value: Template{
											Expr: "user_tb_${ID}_${region}_${index}_${ID2}_${Region2}_${Index2}",
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
			assert.EqualExportedValues(t, tt.getWantRules(t, cfg.Placeholders, cfg.Datasources, cfg.Databases, cfg.Tables), cfg.Rules)
		})
	}
}

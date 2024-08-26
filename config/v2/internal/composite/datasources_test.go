package composite

import (
	"testing"

	"github.com/meoying/dbproxy/config/v2/internal/errs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func TestDatasources(t *testing.T) {

	tests := []struct {
		name     string
		yamlData string

		want        Datasources
		assertError assert.ErrorAssertionFunc
	}{

		{
			name: "仅有一主",
			yamlData: `
datasources:
  master:
    master: webook:webook@tcp(cn.tob.mysql.meoying.com:3306)/order?xxx
 `,
			want: Datasources{
				Variables: map[string]Datasource{
					"master": {
						MasterSlaves: MasterSlaves{
							Master: "webook:webook@tcp(cn.tob.mysql.meoying.com:3306)/order?xxx",
						},
					},
				},
			},
			assertError: assert.NoError,
		},
		{
			name: "一主一从",
			yamlData: `
datasources:
  cn:
    master: webook:webook@tcp(cn.tob.mysql.meoying.com:3306)/order?xxx
    slaves:
      - webook:webook@tcp(cn.slave.toB.mysql.meoying.com:3306)/order?xxx
 `,
			want: Datasources{
				Variables: map[string]Datasource{
					"cn": {
						MasterSlaves: MasterSlaves{
							Master: "webook:webook@tcp(cn.tob.mysql.meoying.com:3306)/order?xxx",
							Slaves: Enum{"webook:webook@tcp(cn.slave.toB.mysql.meoying.com:3306)/order?xxx"},
						},
					},
				},
			},
			assertError: assert.NoError,
		},
		{
			name: "一主多从",
			yamlData: `
datasources:
  cn:
    master: webook:webook@tcp(cn.toB.mysql.meoying.com:3306)/order?xxx
    slaves:
      - webook:webook@tcp(0.cn.slave.toB.mysql.meoying.com:3306)/order?xxx
      - webook:webook@tcp(1.cn.slave.toB.mysql.meoying.com:3306)/order?xxx
 `,
			want: Datasources{
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
				},
			},
			assertError: assert.NoError,
		},
		{
			name: "多个数据源",
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
 `,
			want: Datasources{
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
			assertError: assert.NoError,
		},
		{
			name: "模版语法",
			yamlData: `
datasources:
  hk_equal:
    template:
      master: webook:webook@tcp(${region}.${id}.${role}.${type}.meoying.com:3306)/order?xxx
      slaves: webook:webook@tcp(${region}.${id}.${role}.${type}.meoying.com:3306)/order?xxx
      placeholders:
        region:
          - hk
          - cn
        id:
          hash:
            key: user_id
            base: 3
        role:
          - test
          - prod
        type: mysql
 `,
			want: Datasources{
				Variables: map[string]Datasource{
					"hk_equal": {
						Template: DatasourceTemplate{
							Master: Template{
								Expr: "webook:webook@tcp(${region}.${id}.${role}.${type}.meoying.com:3306)/order?xxx",
								Placeholders: Section[Placeholder]{
									Variables: map[string]Placeholder{
										"region": {
											Value: Enum{"hk", "cn"},
										},
										"id": {
											Value: Hash{Key: "user_id", Base: 3},
										},
										"role": {
											Value: Enum{"test", "prod"},
										},
										"type": {
											Value: String("mysql"),
										},
									},
								},
							},
							Slaves: Template{
								Expr: "webook:webook@tcp(${region}.${id}.${role}.${type}.meoying.com:3306)/order?xxx",
								Placeholders: Section[Placeholder]{
									Variables: map[string]Placeholder{
										"region": {
											Value: Enum{"hk", "cn"},
										},
										"id": {
											Value: Hash{Key: "user_id", Base: 3},
										},
										"role": {
											Value: Enum{"test", "prod"},
										},
										"type": {
											Value: String("mysql"),
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
			name: "模版语法_模版占位符列表中变量引用全局placeholders中变量",
			yamlData: `
placeholders:
  region:
    - hk
    - cn
  role:
    - test
    - prod
  id:
    hash:
      key: user_id
      base: 3
datasources:
  hk_equal:
    template:
      master: webook:webook@tcp(${region}.${id}.${role}.${type}.meoying.com:3306)/order?xxx
      slaves: webook:webook@tcp(${region}.${id}.${role}.${type}.meoying.com:3306)/order?xxx
      placeholders:
        region:
          ref:
            - placeholders.region
        id:
          ref:
            - placeholders.id
        role:
          ref:
            - placeholders.role
        type: mysql
 `,
			want: Datasources{
				Variables: map[string]Datasource{
					"hk_equal": {
						Template: DatasourceTemplate{
							Master: Template{
								Expr: "webook:webook@tcp(${region}.${id}.${role}.${type}.meoying.com:3306)/order?xxx",
								Placeholders: Section[Placeholder]{
									Variables: map[string]Placeholder{
										"region": {
											Value: Enum{"hk", "cn"},
										},
										"id": {
											Value: Hash{Key: "user_id", Base: 3},
										},
										"role": {
											Value: Enum{"test", "prod"},
										},
										"type": {
											Value: String("mysql"),
										},
									},
								},
							},
							Slaves: Template{
								Expr: "webook:webook@tcp(${region}.${id}.${role}.${type}.meoying.com:3306)/order?xxx",
								Placeholders: Section[Placeholder]{
									Variables: map[string]Placeholder{
										"region": {
											Value: Enum{"hk", "cn"},
										},
										"id": {
											Value: Hash{Key: "user_id", Base: 3},
										},
										"role": {
											Value: Enum{"test", "prod"},
										},
										"type": {
											Value: String("mysql"),
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
			name: "全局不支持引用变量",
			yamlData: `
datasources:
  hk_ref:
    ref:
      - datasources.cn
  cn:
    master: webook:webook@tcp(cn.tob.mysql.meoying.com:3306)/order?xxx
 `,
			want: Datasources{},
			assertError: func(t assert.TestingT, err error, i ...interface{}) bool {
				return assert.ErrorIs(t, err, errs.ErrVariableTypeInvalid)
			},
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
			assert.EqualExportedValues(t, tt.want, *cfg.Datasources)
		})
	}
}

func TestDatasources_Evaluate(t *testing.T) {

	tests := []struct {
		name     string
		yamlData string

		want        map[string]MasterSlaves
		assertError assert.ErrorAssertionFunc
	}{

		{
			name: "仅有一主",
			yamlData: `
datasources:
  cn:
    master: webook:webook@tcp(cn.tob.mysql.meoying.com:3306)/order?xxx
 `,
			want: map[string]MasterSlaves{
				"cn": {
					Master: String("webook:webook@tcp(cn.tob.mysql.meoying.com:3306)/order?xxx"),
				},
			},
			assertError: assert.NoError,
		},
		{
			name: "一主一从",
			yamlData: `
datasources:
  cn:
    master: webook:webook@tcp(cn.tob.mysql.meoying.com:3306)/order?xxx
    slaves:
      - webook:webook@tcp(cn.slave.toB.mysql.meoying.com:3306)/order?xxx
 `,
			want: map[string]MasterSlaves{
				"cn": {
					Master: String("webook:webook@tcp(cn.tob.mysql.meoying.com:3306)/order?xxx"),
					Slaves: Enum{"webook:webook@tcp(cn.slave.toB.mysql.meoying.com:3306)/order?xxx"},
				},
			},
			assertError: assert.NoError,
		},
		{
			name: "一主多从",
			yamlData: `
datasources:
  cn:
    master: webook:webook@tcp(cn.toB.mysql.meoying.com:3306)/order?xxx
    slaves:
      - webook:webook@tcp(0.cn.slave.toB.mysql.meoying.com:3306)/order?xxx
      - webook:webook@tcp(1.cn.slave.toB.mysql.meoying.com:3306)/order?xxx
 `,
			want: map[string]MasterSlaves{
				"cn": {
					Master: String("webook:webook@tcp(cn.toB.mysql.meoying.com:3306)/order?xxx"),
					Slaves: Enum{
						"webook:webook@tcp(0.cn.slave.toB.mysql.meoying.com:3306)/order?xxx",
						"webook:webook@tcp(1.cn.slave.toB.mysql.meoying.com:3306)/order?xxx",
					},
				},
			},
			assertError: assert.NoError,
		},
		{
			name: "多个数据源",
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
 `,
			want: map[string]MasterSlaves{
				"cn_test": {
					Master: String("webook:webook@tcp(cn.master.test.mysql.meoying.com:3306)/order?xxx"),
					Slaves: Enum{
						"webook:webook@tcp(cn.slave.test.mysql.meoying.com:3306)/order?xxx",
					},
				},
				"hk_test": {
					Master: String("webook:webook@tcp(hk.master.test.mysql.meoying.com:3306)/order?xxx"),
					Slaves: Enum{
						"webook:webook@tcp(hk.slave.test.mysql.meoying.com:3306)/order?xxx",
					},
				},
				"cn_prod": {
					Master: String("webook:webook@tcp(cn.master.prod.mysql.meoying.com:3306)/order?xxx"),
					Slaves: Enum{
						"webook:webook@tcp(cn.slave.prod.mysql.meoying.com:3306)/order?xxx",
					},
				},
				"hk_prod": {
					Master: String("webook:webook@tcp(hk.master.prod.mysql.meoying.com:3306)/order?xxx"),
					Slaves: Enum{
						"webook:webook@tcp(hk.slave.prod.mysql.meoying.com:3306)/order?xxx",
					},
				},
			},
			assertError: assert.NoError,
		},
		{
			name: "模版语法",
			yamlData: `
datasources:
  hk_equal:
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
			want: map[string]MasterSlaves{
				"cn_test": {
					Master: String("webook:webook@tcp(cn.master.test.mysql.meoying.com:3306)/order?xxx"),
					Slaves: Enum{
						"webook:webook@tcp(cn.slave.test.mysql.meoying.com:3306)/order?xxx",
					},
				},
				"hk_test": {
					Master: String("webook:webook@tcp(hk.master.test.mysql.meoying.com:3306)/order?xxx"),
					Slaves: Enum{
						"webook:webook@tcp(hk.slave.test.mysql.meoying.com:3306)/order?xxx",
					},
				},
				"cn_prod": {
					Master: String("webook:webook@tcp(cn.master.prod.mysql.meoying.com:3306)/order?xxx"),
					Slaves: Enum{
						"webook:webook@tcp(cn.slave.prod.mysql.meoying.com:3306)/order?xxx",
					},
				},
				"hk_prod": {
					Master: String("webook:webook@tcp(hk.master.prod.mysql.meoying.com:3306)/order?xxx"),
					Slaves: Enum{
						"webook:webook@tcp(hk.slave.prod.mysql.meoying.com:3306)/order?xxx",
					},
				},
			},
			assertError: assert.NoError,
		},
		// TODO: 模版中引用全局placeholders中的变量
		{
			name: "模版语法_模版占位符列表中变量引用全局placeholders中变量",
			yamlData: `
placeholders:
  region:
    - hk
    - cn
  role:
    - test
    - prod
  id:
    hash:
      key: user_id
      base: 3
datasources:
  hk_equal:
    template:
      master: webook:webook@tcp(${region}.${id}.${role}.${type}.master.meoying.com:3306)/order?xxx
      slaves: webook:webook@tcp(${region}.${id}.${role}.${type}.slave.meoying.com:3306)/order?xxx
      placeholders:
        region:
          ref:
            - placeholders.region
        id:
          ref:
            - placeholders.id
        role:
          ref:
            - placeholders.role
        type: mysql
 `,
			want: map[string]MasterSlaves{
				"cn_0_test_mysql": {
					Master: String("webook:webook@tcp(cn.0.test.mysql.master.meoying.com:3306)/order?xxx"),
					Slaves: Enum{
						"webook:webook@tcp(cn.0.test.mysql.slave.meoying.com:3306)/order?xxx",
					},
				},
				"cn_1_test_mysql": {
					Master: String("webook:webook@tcp(cn.1.test.mysql.master.meoying.com:3306)/order?xxx"),
					Slaves: Enum{
						"webook:webook@tcp(cn.1.test.mysql.slave.meoying.com:3306)/order?xxx",
					},
				},
				"cn_2_test_mysql": {
					Master: String("webook:webook@tcp(cn.2.test.mysql.master.meoying.com:3306)/order?xxx"),
					Slaves: Enum{
						"webook:webook@tcp(cn.2.test.mysql.slave.meoying.com:3306)/order?xxx",
					},
				},
				"cn_0_prod_mysql": {
					Master: String("webook:webook@tcp(cn.0.prod.mysql.master.meoying.com:3306)/order?xxx"),
					Slaves: Enum{
						"webook:webook@tcp(cn.0.prod.mysql.slave.meoying.com:3306)/order?xxx",
					},
				},
				"cn_1_prod_mysql": {
					Master: String("webook:webook@tcp(cn.1.prod.mysql.master.meoying.com:3306)/order?xxx"),
					Slaves: Enum{
						"webook:webook@tcp(cn.1.prod.mysql.slave.meoying.com:3306)/order?xxx",
					},
				},
				"cn_2_prod_mysql": {
					Master: String("webook:webook@tcp(cn.2.prod.mysql.master.meoying.com:3306)/order?xxx"),
					Slaves: Enum{
						"webook:webook@tcp(cn.2.prod.mysql.slave.meoying.com:3306)/order?xxx",
					},
				},
				"hk_0_test_mysql": {
					Master: String("webook:webook@tcp(hk.0.test.mysql.master.meoying.com:3306)/order?xxx"),
					Slaves: Enum{
						"webook:webook@tcp(hk.0.test.mysql.slave.meoying.com:3306)/order?xxx",
					},
				},
				"hk_1_test_mysql": {
					Master: String("webook:webook@tcp(hk.1.test.mysql.master.meoying.com:3306)/order?xxx"),
					Slaves: Enum{
						"webook:webook@tcp(hk.1.test.mysql.slave.meoying.com:3306)/order?xxx",
					},
				},
				"hk_2_test_mysql": {
					Master: String("webook:webook@tcp(hk.2.test.mysql.master.meoying.com:3306)/order?xxx"),
					Slaves: Enum{
						"webook:webook@tcp(hk.2.test.mysql.slave.meoying.com:3306)/order?xxx",
					},
				},
				"hk_0_prod_mysql": {
					Master: String("webook:webook@tcp(hk.0.prod.mysql.master.meoying.com:3306)/order?xxx"),
					Slaves: Enum{
						"webook:webook@tcp(hk.0.prod.mysql.slave.meoying.com:3306)/order?xxx",
					},
				},
				"hk_1_prod_mysql": {
					Master: String("webook:webook@tcp(hk.1.prod.mysql.master.meoying.com:3306)/order?xxx"),
					Slaves: Enum{
						"webook:webook@tcp(hk.1.prod.mysql.slave.meoying.com:3306)/order?xxx",
					},
				},
				"hk_2_prod_mysql": {
					Master: String("webook:webook@tcp(hk.2.prod.mysql.master.meoying.com:3306)/order?xxx"),
					Slaves: Enum{
						"webook:webook@tcp(hk.2.prod.mysql.slave.meoying.com:3306)/order?xxx",
					},
				},
			},
			assertError: assert.NoError,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var cfg Config
			cfg.testMode = true
			err := yaml.Unmarshal([]byte(tt.yamlData), &cfg)
			require.NoError(t, err)

			actual, err := cfg.Datasources.Evaluate()
			tt.assertError(t, err)
			if err != nil {
				return
			}
			assert.Equal(t, tt.want, actual)
		})
	}
}

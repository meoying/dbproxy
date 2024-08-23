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

		getWantRules func(t *testing.T, ds *Datasources, db *Databases, tb *Tables) Rules
		assertError  assert.ErrorAssertionFunc
	}{
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
			getWantRules: func(t *testing.T, ds *Datasources, db *Databases, tb *Tables) Rules {
				return Rules{
					Variables: map[string]Rule{
						"user": {
							globalDatasources: ds,
							Datasources: Datasources{
								Variables: map[string]Datasource{
									"cn": {
										Master: "webook:webook@tcp(cn.toB.mysql.meoying.com:3306)/order?xxx",
										Slaves: Enum{
											"webook:webook@tcp(0.cn.slave.toB.mysql.meoying.com:3306)/order?xxx",
											"webook:webook@tcp(1.cn.slave.toB.mysql.meoying.com:3306)/order?xxx",
										},
									},
									"hk": {
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
				}
			},
			assertError: assert.NoError,
		},
		{
			// TODO: 后续改为报错,必须包含数据源、数据库、数据表
			name: "仅有局部数据源定义_引用全局变量_数据源类型",
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
			getWantRules: func(t *testing.T, ds *Datasources, db *Databases, tb *Tables) Rules {
				return Rules{
					Datasources: ds,
					Variables: map[string]Rule{
						"user": {
							globalDatasources: ds,
							Datasources: Datasources{
								global: ds,
								Variables: map[string]Datasource{
									"cn_test": {
										Master: "webook:webook@tcp(cn.master.test.mysql.meoying.com:3306)/order?xxx",
										Slaves: Enum{
											"webook:webook@tcp(cn.slave.test.mysql.meoying.com:3306)/order?xxx",
										},
									},
									"hk_test": {
										Master: "webook:webook@tcp(hk.master.test.mysql.meoying.com:3306)/order?xxx",
										Slaves: Enum{
											"webook:webook@tcp(hk.slave.test.mysql.meoying.com:3306)/order?xxx",
										},
									},
									"cn_prod": {
										Master: "webook:webook@tcp(cn.master.prod.mysql.meoying.com:3306)/order?xxx",
										Slaves: Enum{
											"webook:webook@tcp(cn.slave.prod.mysql.meoying.com:3306)/order?xxx",
										},
									},
									"hk_prod": {
										Master: "webook:webook@tcp(hk.master.prod.mysql.meoying.com:3306)/order?xxx",
										Slaves: Enum{
											"webook:webook@tcp(hk.slave.prod.mysql.meoying.com:3306)/order?xxx",
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
		// 		{
		// 			// TODO: 后续改为报错,必须包含数据源、数据库、数据表
		// 			name: "仅有局部数据源定义_引用全局变量_模版类型",
		// 			yamlData: `
		// datasources:
		//   ds_tmpl:
		//     template:
		//       master: webook:webook@tcp(${region}.master.${role}.mysql.meoying.com:3306)/order?xxx
		//       slaves: webook:webook@tcp(${region}.slave.${role}.mysql.meoying.com:3306)/order?xxx
		//       placeholders:
		//         region:
		//           - hk
		//           - cn
		//         role:
		//           - test
		//           - prod
		// rules:
		//   user:
		//     datasources:
		//       ref:
		//         - datasources.ds_tmpl
		// `,
		// 			getWantRules: func(t *testing.T, ds *Datasources, db *Databases, tb *Tables) Rules {
		// 				return Rules{
		// 					Datasources: ds,
		// 					Variables: map[string]Rule{
		// 						"user": {
		// 							globalDatasources: ds,
		// 							Datasources: Datasources{
		// 								global: ds,
		// 								Variables: map[string]Datasource{
		// 									"cn_test": {
		// 										Master: "webook:webook@tcp(cn.master.test.mysql.meoying.com:3306)/order?xxx",
		// 										Slaves: Enum{
		// 											"webook:webook@tcp(cn.slave.test.mysql.meoying.com:3306)/order?xxx",
		// 										},
		// 									},
		// 									"hk_test": {
		// 										Master: "webook:webook@tcp(hk.master.test.mysql.meoying.com:3306)/order?xxx",
		// 										Slaves: Enum{
		// 											"webook:webook@tcp(hk.slave.test.mysql.meoying.com:3306)/order?xxx",
		// 										},
		// 									},
		// 									"cn_prod": {
		// 										Master: "webook:webook@tcp(cn.master.prod.mysql.meoying.com:3306)/order?xxx",
		// 										Slaves: Enum{
		// 											"webook:webook@tcp(cn.slave.prod.mysql.meoying.com:3306)/order?xxx",
		// 										},
		// 									},
		// 									"hk_prod": {
		// 										Master: "webook:webook@tcp(hk.master.prod.mysql.meoying.com:3306)/order?xxx",
		// 										Slaves: Enum{
		// 											"webook:webook@tcp(hk.slave.prod.mysql.meoying.com:3306)/order?xxx",
		// 										},
		// 									},
		// 								},
		// 							},
		// 						},
		// 					},
		// 				}
		// 			},
		// 			assertError: assert.NoError,
		// 		},
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
			getWantRules: func(t *testing.T, ds *Datasources, db *Databases, tb *Tables) Rules {
				return Rules{}
			},
			assertError: func(t assert.TestingT, err error, i ...interface{}) bool {
				return assert.ErrorIs(t, err, errs.ErrReferencePathInvalid)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var cfg Config
			err := yaml.Unmarshal([]byte(tt.yamlData), &cfg)
			tt.assertError(t, err)
			if err != nil {
				return
			}
			assert.Equal(t, tt.getWantRules(t, cfg.Datasources, nil, nil), cfg.Rules)
		})
	}
}

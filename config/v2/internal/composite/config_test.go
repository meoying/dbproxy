package composite

import (
	"testing"

	"github.com/meoying/dbproxy/config/v2/internal/errs"
	"github.com/stretchr/testify/assert"
	"gopkg.in/yaml.v3"
)

func TestConfig(t *testing.T) {
	tests := []struct {
		name     string
		yamlData string

		getWantFunc func(t *testing.T, ph *Section[Placeholder], ds *Section[Datasource], db *Section[Database], tb *Section[Table]) Config
		assertError assert.ErrorAssertionFunc
	}{
		{
			name: "不分库分表",
			yamlData: `
rules:
  user:
    datasources: webook:webook@tcp(cn.mysql.meoying.com:3306)/?xxx
    databases: user_db
    tables: user_tb
`,
			getWantFunc: func(t *testing.T, ph *Section[Placeholder], ds *Section[Datasource], db *Section[Database], tb *Section[Table]) Config {
				return Config{
					Placeholders: ph,
					Datasources:  ds,
					Databases:    db,
					Tables:       tb,
					Rules: Rules{
						Variables: map[string]Rule{
							"user": {
								Datasources: Section[Datasource]{
									Variables: map[string]Datasource{
										"": {
											Value: String("webook:webook@tcp(cn.mysql.meoying.com:3306)/?xxx"),
										},
									},
								},
								Databases: Section[Database]{
									Variables: map[string]Database{
										"": {
											String("user_db"),
										},
									},
								},
								Tables: Section[Table]{
									Variables: map[string]Table{
										"": {
											String("user_tb"),
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
			name: "不分库分表_命名单主",
			yamlData: `
rules:
  user:
    datasources:
      cn:
        master: webook:webook@tcp(cn.mysql.meoying.com:3306)/?xxx
    databases: user_db
    tables: user_tb
`,
			getWantFunc: func(t *testing.T, ph *Section[Placeholder], ds *Section[Datasource], db *Section[Database], tb *Section[Table]) Config {
				return Config{
					Placeholders: ph,
					Datasources:  ds,
					Databases:    db,
					Tables:       tb,
					Rules: Rules{
						Variables: map[string]Rule{
							"user": {
								Datasources: Section[Datasource]{
									Variables: map[string]Datasource{
										"cn": {
											Value: MasterSlaves{
												Master: "webook:webook@tcp(cn.mysql.meoying.com:3306)/?xxx",
											},
										},
									},
								},
								Databases: Section[Database]{
									Variables: map[string]Database{
										"": {
											String("user_db"),
										},
									},
								},
								Tables: Section[Table]{
									Variables: map[string]Table{
										"": {
											String("user_tb"),
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
			name: "不分库分表_匿名单主",
			yamlData: `
rules:
  user:
    datasources:
      master: webook:webook@tcp(cn.mysql.meoying.com:3306)/?xxx
    databases: user_db
    tables: user_tb
`,
			getWantFunc: func(t *testing.T, ph *Section[Placeholder], ds *Section[Datasource], db *Section[Database], tb *Section[Table]) Config {
				return Config{
					Placeholders: ph,
					Datasources:  ds,
					Databases:    db,
					Tables:       tb,
					Rules: Rules{
						Variables: map[string]Rule{
							"user": {
								Datasources: Section[Datasource]{
									Variables: map[string]Datasource{
										"": {
											Value: MasterSlaves{
												Master: "webook:webook@tcp(cn.mysql.meoying.com:3306)/?xxx",
											},
										},
									},
								},
								Databases: Section[Database]{
									Variables: map[string]Database{
										"": {
											String("user_db"),
										},
									},
								},
								Tables: Section[Table]{
									Variables: map[string]Table{
										"": {
											String("user_tb"),
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
			name: "分库分表_简单情况",
			yamlData: `
rules:
  user:
    datasources:
      cn:
        master: webook:webook@tcp(cn.mysql.meoying.com:3306)/?xxx
      hk:
        master: webook:webook@tcp(hk.mysql.meoying.com:3306)/?xxx
    databases:
       template:
         expr: user_db_${key}
         placeholders:
           key:
             hash:
               key: user_id
               base: 3
    tables:
      - user_tb_0
      - user_tb_1
`,
			getWantFunc: func(t *testing.T, ph *Section[Placeholder], ds *Section[Datasource], db *Section[Database], tb *Section[Table]) Config {
				return Config{
					Placeholders: ph,
					Datasources:  ds,
					Databases:    db,
					Tables:       tb,
					Rules: Rules{
						Variables: map[string]Rule{
							"user": {
								Datasources: Section[Datasource]{
									Variables: map[string]Datasource{
										"cn": {
											Value: MasterSlaves{
												Master: "webook:webook@tcp(cn.mysql.meoying.com:3306)/?xxx",
											},
										},
										"hk": {
											Value: MasterSlaves{
												Master: "webook:webook@tcp(hk.mysql.meoying.com:3306)/?xxx",
											},
										},
									},
								},
								Databases: Section[Database]{
									Variables: map[string]Database{
										"template": {
											Template{
												Expr: "user_db_${key}",
												Placeholders: Section[Placeholder]{
													Variables: map[string]Placeholder{
														"key": {
															Value: Hash{Key: "user_id", Base: 3},
														},
													},
												},
											},
										},
									},
								},
								Tables: Section[Table]{
									Variables: map[string]Table{
										"": {
											Enum{"user_tb_0", "user_tb_1"},
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
			err := yaml.Unmarshal([]byte(tt.yamlData), &cfg)
			tt.assertError(t, err)
			if err != nil {
				return
			}
			want := tt.getWantFunc(t, cfg.Placeholders, cfg.Datasources, cfg.Databases, cfg.Tables)
			assert.EqualExportedValues(t, want, cfg)
		})
	}
}

func TestConfig_UnmarshalError(t *testing.T) {
	var cfg Config
	err := yaml.Unmarshal([]byte(`str`), &cfg)
	assert.ErrorIs(t, err, errs.ErrConfigSyntaxInvalid)
}

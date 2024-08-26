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

		want        Config
		assertError assert.ErrorAssertionFunc
	}{
		{
			name: "不分库分表",
			yamlData: `
rules:
  user:
    datasources:
      cn:
        master: webook:webook@tcp(cn.mysql.meoying.com:3306)/?xxx
    databases: user_db
    tables: user_tb
`,
			want: Config{
				Rules: Rules{
					Variables: map[string]Rule{
						"user": {
							Datasources: Datasources{
								Variables: map[string]Datasource{
									"cn": {
										MasterSlaves: MasterSlaves{
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
			want: Config{
				Rules: Rules{
					Variables: map[string]Rule{
						"user": {
							Datasources: Datasources{
								Variables: map[string]Datasource{
									"cn": {
										MasterSlaves: MasterSlaves{
											Master: "webook:webook@tcp(cn.mysql.meoying.com:3306)/?xxx",
										},
									},
									"hk": {
										MasterSlaves: MasterSlaves{
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
			},
			assertError: assert.NoError,
		},
	}

	for _, tt := range tests {
		var cfg Config
		err := yaml.Unmarshal([]byte(tt.yamlData), &cfg)
		tt.assertError(t, err)
		if err != nil {
			return
		}
		assert.True(t, cfg.Datasources.IsZero(), cfg.Datasources)
		cfg.Datasources = nil
		assert.True(t, cfg.Databases.IsZero(), cfg.Databases)
		cfg.Databases = nil
		assert.True(t, cfg.Tables.IsZero(), cfg.Tables)
		cfg.Tables = nil
		assert.True(t, cfg.Placeholders.IsZero(), cfg.Placeholders)
		cfg.Placeholders = nil
		assert.EqualExportedValues(t, tt.want, cfg)
	}
}

func TestConfig_UnmarshalError(t *testing.T) {
	var cfg Config
	err := yaml.Unmarshal([]byte(`str`), &cfg)
	assert.ErrorIs(t, err, errs.ErrConfigSyntaxInvalid)
}

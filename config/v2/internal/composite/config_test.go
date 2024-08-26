package composite

import (
	"testing"

	"github.com/meoying/dbproxy/config/v2/internal/errs"
	"github.com/stretchr/testify/assert"
	"gopkg.in/yaml.v3"
)

func TestConfig(t *testing.T) {
	t.Skip()
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
				Placeholders: nil,
				Datasources:  nil,
				Databases:    nil,
				Tables:       nil,
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
	}

	for _, tt := range tests {
		var cfg Config
		err := yaml.Unmarshal([]byte(tt.yamlData), &cfg)
		tt.assertError(t, err)
		if err != nil {
			return
		}
		assert.EqualExportedValues(t, tt.want, cfg)
	}
}

func TestConfig_UnmarshalError(t *testing.T) {
	var cfg Config
	err := yaml.Unmarshal([]byte(`str`), &cfg)
	assert.ErrorIs(t, err, errs.ErrConfigSyntaxInvalid)
}

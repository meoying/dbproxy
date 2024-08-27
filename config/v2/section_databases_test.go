package v2

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"gopkg.in/yaml.v3"
)

func TestDatabases(t *testing.T) {
	tests := []struct {
		name     string
		yamlData string

		want        Section[Database]
		assertError assert.ErrorAssertionFunc
	}{
		{
			name: "字符串类型",
			yamlData: `
databases:
  user: user_db
`,
			want: Section[Database]{
				Variables: map[string]Database{
					"user": {
						Value: String("user_db"),
					},
				},
			},
			assertError: assert.NoError,
		},
		{
			name: "枚举类型",
			yamlData: `
databases:
  order:
    - order_db_0
  payment:
    - payment_db_0
    - payment_db_1
`,
			want: Section[Database]{
				Variables: map[string]Database{
					"order": {
						Value: Enum{"order_db_0"},
					},
					"payment": {
						Value: Enum{"payment_db_0", "payment_db_1"},
					},
				},
			},
			assertError: assert.NoError,
		},
		{
			name: "哈希类型",
			yamlData: `
databases:
  hash:
    hash:
      key: user_id
      base: 3
`,
			want: Section[Database]{
				Variables: map[string]Database{
					"hash": {
						Value: Hash{
							Key:  "user_id",
							Base: 3,
						},
					},
				},
			},
			assertError: assert.NoError,
		},
		{
			name: "模版类型",
			yamlData: `
databases:
  payment:
    template:
      expr: payment_db_${ID}
      placeholders:
        ID:
          - 0
          - 1
`,
			want: Section[Database]{
				Variables: map[string]Database{
					"payment": {
						Value: Template{
							Expr: "payment_db_${ID}",
							Placeholders: Section[Placeholder]{
								Variables: map[string]Placeholder{
									"ID": {
										Value: Enum{"0", "1"},
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
			name: "模版类型_引用全局占位符",
			yamlData: `
placeholders:
  name: db
  id:
    - 0
    - 1
  index:
    hash:
      key: user_id
      base: 3
databases:
  payment:
    template:
      expr: payment_${name}_${ID}_${index}
      placeholders:
        name:
          ref:
            - placeholders.name
        ID:
          ref:
            - placeholders.id
        index:
          ref:
            - placeholders.index
`,
			want: Section[Database]{
				Variables: map[string]Database{
					"payment": {
						Value: Template{
							Expr: "payment_${name}_${ID}_${index}",
							Placeholders: Section[Placeholder]{
								Variables: map[string]Placeholder{
									"name": {
										Value: String("db"),
									},
									"ID": {
										Value: Enum{"0", "1"},
									},
									"index": {
										Value: Hash{Key: "user_id", Base: 3},
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
			name: "应该报错_不支持引用全局变量",
			yamlData: `
databases:
  region_ref:
    ref:
      - databases.region
  region:
    - hk
    - uk
 `,
			want: Section[Database]{},
			assertError: func(t assert.TestingT, err error, i ...any) bool {
				return assert.ErrorIs(t, err, ErrVariableTypeInvalid)
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
			assert.EqualExportedValues(t, tt.want, *cfg.Databases)
		})
	}
}

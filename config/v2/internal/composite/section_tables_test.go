package composite

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"gopkg.in/yaml.v3"
)

func TestTables(t *testing.T) {
	tests := []struct {
		name     string
		yamlData string

		want        Section[Table]
		assertError assert.ErrorAssertionFunc
	}{
		{
			name: "字符串类型",
			yamlData: `
tables:
  user: user_db
`,
			want: Section[Table]{
				Variables: map[string]Table{
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
tables:
  order:
    - order_db_0
  payment:
    - payment_db_0
    - payment_db_1
`,
			want: Section[Table]{
				Variables: map[string]Table{
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
tables:
  hash:
    hash:
      key: user_id
      base: 3
`,
			want: Section[Table]{
				Variables: map[string]Table{
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
tables:
  payment:
    template:
      expr: payment_db_${ID}
      placeholders:
        ID:
          - 0
          - 1
`,
			want: Section[Table]{
				Variables: map[string]Table{
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
tables:
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
			want: Section[Table]{
				Variables: map[string]Table{
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
			assert.EqualExportedValues(t, tt.want, *cfg.Tables)
		})
	}
}

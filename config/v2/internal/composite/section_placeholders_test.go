package composite

import (
	"testing"

	"github.com/meoying/dbproxy/config/v2/internal/errs"
	"github.com/stretchr/testify/assert"
	"gopkg.in/yaml.v3"
)

func TestPlaceholders(t *testing.T) {
	tests := []struct {
		name     string
		yamlData string

		want        Section[Placeholder]
		assertError assert.ErrorAssertionFunc
	}{
		{
			name: "字符串类型",
			yamlData: `
placeholders:
  str: This is string
`,
			want: Section[Placeholder]{
				Variables: map[string]Placeholder{
					"str": {
						Value: String("This is string"),
					},
				},
			},
			assertError: assert.NoError,
		},
		{
			name: "枚举类型",
			yamlData: `
placeholders:
  enum:
    - hk
    - cn
`,
			want: Section[Placeholder]{
				Variables: map[string]Placeholder{
					"enum": {
						Value: Enum{"hk", "cn"},
					},
				},
			},
			assertError: assert.NoError,
		},
		{
			name: "哈希类型",
			yamlData: `
placeholders:
  hash:
    hash:
      key: user_id
      base: 3
`,
			want: Section[Placeholder]{
				Variables: map[string]Placeholder{
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
			name: "应该报错_模版类型",
			yamlData: `
placeholders:
  tmpl:
    template:
      expr: order_db_${key}
      placeholders:
        key:
          - 0
          - 1
`,
			want: Section[Placeholder]{},
			assertError: func(t assert.TestingT, err error, i ...any) bool {
				return assert.ErrorIs(t, err, errs.ErrVariableTypeInvalid)
			},
		},
		{
			name: "应该报错_不支持全局自引用",
			yamlData: `
placeholders:
  region_ref:
    ref:
      - placeholders.region
  region:
    - hk
    - uk
 `,
			want: Section[Placeholder]{},
			assertError: func(t assert.TestingT, err error, i ...any) bool {
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
			assert.EqualExportedValues(t, tt.want, *cfg.Placeholders)
		})
	}
}

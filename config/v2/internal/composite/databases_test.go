package composite

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"gopkg.in/yaml.v3"
)

func TestDatabases(t *testing.T) {
	tests := []struct {
		name     string
		yamlData string

		getWantFunc func(t *testing.T, ph *Placeholders) Section[Database]
		assertError assert.ErrorAssertionFunc
	}{
		{
			name: "字符串类型",
			yamlData: `
databases:
  user: user_db
`,
			getWantFunc: func(t *testing.T, ph *Placeholders) Section[Database] {
				return Section[Database]{
					Variables: map[string]Database{
						"user": {
							Value: String("user_db"),
						},
					},
				}
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
			getWantFunc: func(t *testing.T, ph *Placeholders) Section[Database] {
				return Section[Database]{
					Variables: map[string]Database{
						"order": {
							Value: Enum{"order_db_0"},
						},
						"payment": {
							Value: Enum{"payment_db_0", "payment_db_1"},
						},
					},
				}
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
			getWantFunc: func(t *testing.T, ph *Placeholders) Section[Database] {
				return Section[Database]{
					Variables: map[string]Database{
						"hash": {
							Value: Hash{
								Key:  "user_id",
								Base: 3,
							},
						},
					},
				}
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
			getWantFunc: func(t *testing.T, ph *Placeholders) Section[Database] {
				return Section[Database]{
					Variables: map[string]Database{
						"payment": {
							Value: Template{
								Expr: "payment_db_${ID}",
								Placeholders: Placeholders{
									Variables: map[string]Placeholder{
										"ID": {
											Enum: Enum{"0", "1"},
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
		// 模版类型_引用全局占位符_字符串
		{
			name: "模版类型_引用全局占位符_枚举",
			yamlData: `
placeholders:
  id:
    - 0
    - 1
databases:
  payment:
    template:
      expr: payment_db_${ID}
      placeholders:
        ID:
          ref:
            - placeholders.id 
`,
			getWantFunc: func(t *testing.T, ph *Placeholders) Section[Database] {
				return Section[Database]{
					Variables: map[string]Database{
						"payment": {
							Value: Template{
								Expr: "payment_db_${ID}",
								Placeholders: Placeholders{
									Variables: map[string]Placeholder{
										"ID": {
											Enum: Enum{"0", "1"},
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
		// 模版类型_引用全局占位符_哈希
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
			assert.EqualExportedValues(t, tt.getWantFunc(t, cfg.Placeholders), *cfg.Databases)
		})
	}
}

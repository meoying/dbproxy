package composite

import (
	"fmt"
	"log"
	"testing"

	"github.com/stretchr/testify/assert"
	"gopkg.in/yaml.v3"
)

func TestTables(t *testing.T) {
	tests := []struct {
		name     string
		yamlData string

		getWantFunc func(t *testing.T, ph *Placeholders) Section[Table]
		assertError assert.ErrorAssertionFunc
	}{
		{
			name: "字符串类型",
			yamlData: `
tables:
  user: user_db
`,
			getWantFunc: func(t *testing.T, ph *Placeholders) Section[Table] {
				return Section[Table]{
					globalPlaceholders: ph,
					Variables: map[string]Table{
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
tables:
  order:
    - order_db_0
  payment:
    - payment_db_0
    - payment_db_1
`,
			getWantFunc: func(t *testing.T, ph *Placeholders) Section[Table] {
				return Section[Table]{
					globalPlaceholders: ph,
					Variables: map[string]Table{
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
tables:
  hash:
    hash:
      key: user_id
      base: 3
`,
			getWantFunc: func(t *testing.T, ph *Placeholders) Section[Table] {
				return Section[Table]{
					globalPlaceholders: ph,
					Variables: map[string]Table{
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
tables:
  payment:
    template:
      expr: payment_db_${ID}
      placeholders:
        ID:
          - 0
          - 1
`,
			getWantFunc: func(t *testing.T, ph *Placeholders) Section[Table] {
				return Section[Table]{
					globalPlaceholders: ph,
					Variables: map[string]Table{
						"payment": {
							Value: Template{
								global: ph,
								Expr:   "payment_db_${ID}",
								Placeholders: Placeholders{
									global: ph,
									variables: map[string]Placeholder{
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
		{
			name: "模版类型_引用全局占位符",
			yamlData: `
placeholders:
  id:
    - 0
    - 1
tables:
  payment:
    template:
      expr: payment_db_${ID}
      placeholders:
        ID:
          ref:
            - placeholders.id 
`,
			getWantFunc: func(t *testing.T, ph *Placeholders) Section[Table] {
				return Section[Table]{
					globalPlaceholders: ph,
					Variables: map[string]Table{
						"payment": {
							Value: Template{
								global: ph,
								Expr:   "payment_db_${ID}",
								Placeholders: Placeholders{
									global: ph,
									variables: map[string]Placeholder{
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
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var cfg ConfigV1
			cfg.testMode = true
			err := yaml.Unmarshal([]byte(tt.yamlData), &cfg)
			tt.assertError(t, err)
			if err != nil {
				return
			}
			assert.EqualExportedValues(t, tt.getWantFunc(t, cfg.Placeholders), *cfg.Tables)
		})
	}
}

type ConfigV1 struct {
	testMode     bool
	Placeholders *Placeholders   `yaml:"placeholders,omitempty"`
	Datasources  *Datasources    `yaml:"datasources,omitempty"`
	Databases    *Databases      `yaml:"databases,omitempty"`
	Tables       *Section[Table] `yaml:"tables,omitempty"`
	Rules        Rules           `yaml:"rules"`
}

func (c *ConfigV1) UnmarshalYAML(value *yaml.Node) error {

	type rawConfig struct {
		Placeholders *Placeholders   `yaml:"placeholders,omitempty"`
		Datasources  *Datasources    `yaml:"datasources,omitempty"`
		Databases    *Databases      `yaml:"databases,omitempty"`
		Tables       *Section[Table] `yaml:"tables,omitempty"`
		Rules        map[string]any  `yaml:"rules"`
	}
	ph := &Placeholders{}
	raw := rawConfig{
		Placeholders: ph,
		Datasources:  &Datasources{globalPlaceholders: ph},
		Databases:    &Databases{globalPlaceholders: ph},
		Tables:       NewSection[Table](ConfigSectionTypeTables, nil, ph, NewTable),
	}
	err := value.Decode(&raw)
	if err != nil {
		return err
	}

	log.Printf("raw.Config = %#v\n", raw)

	// 全局定义
	c.Placeholders = raw.Placeholders
	c.Datasources = raw.Datasources
	c.Databases = raw.Databases
	c.Tables = raw.Tables

	log.Printf("raw.Config.Rules = %#v\n", raw.Rules)
	c.Rules.placeholders = c.Placeholders
	c.Rules.datasources = c.Datasources
	c.Rules.databases = c.Databases
	// c.Rules.tables = c.Tables
	out, err := yaml.Marshal(raw.Rules)
	if err != nil {
		return err
	}
	err = yaml.Unmarshal(out, &c.Rules)
	if err != nil {
		return err
	}

	if len(c.Rules.Variables) == 0 && !c.testMode {
		return fmt.Errorf("no rules defined")
	}

	return nil
}

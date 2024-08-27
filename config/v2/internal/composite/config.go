package composite

import (
	"fmt"
	"log"

	"github.com/meoying/dbproxy/config/v2/internal/errs"
	"gopkg.in/yaml.v3"
)

type Config struct {
	testMode     bool
	Placeholders *Section[Placeholder] `yaml:"placeholders,omitempty"`
	Datasources  *Section[Datasource]  `yaml:"datasources,omitempty"`
	Databases    *Section[Database]    `yaml:"databases,omitempty"`
	Tables       *Section[Table]       `yaml:"tables,omitempty"`
	Rules        Rules                 `yaml:"rules"`
}

func (c *Config) UnmarshalYAML(value *yaml.Node) error {

	type rawConfig struct {
		Placeholders *Section[Placeholder] `yaml:"placeholders,omitempty"`
		Datasources  *Section[Datasource]  `yaml:"datasources,omitempty"`
		Databases    *Section[Database]    `yaml:"databases,omitempty"`
		Tables       *Section[Table]       `yaml:"tables,omitempty"`
		Rules        map[string]any        `yaml:"rules"`
	}
	ph := NewSection[Placeholder](ConfigSectionTypePlaceholders, nil, nil, NewPlaceholder)
	raw := rawConfig{
		Placeholders: ph,
		Datasources:  NewSection[Datasource](ConfigSectionTypeDatasources, nil, ph, NewDatasource),
		Databases:    NewSection[Database](ConfigSectionTypeDatabases, nil, ph, NewDatabase),
		Tables:       NewSection[Table](ConfigSectionTypeTables, nil, ph, NewTable),
	}
	err := value.Decode(&raw)
	if err != nil {
		return fmt.Errorf("%w: %w", errs.ErrConfigSyntaxInvalid, err)
	}

	log.Printf("raw.Config = %#v\n", raw)

	// 全局预定义的、可选的配置
	// placeholders/datasources/databases/tables
	c.Placeholders = raw.Placeholders
	c.Datasources = raw.Datasources
	c.Databases = raw.Databases
	c.Tables = raw.Tables

	log.Printf("raw.Config.Rules = %#v\n", raw.Rules)
	// 全局预定义的、必选的配置
	// rules
	c.Rules.testMode = c.testMode
	c.Rules.placeholders = c.Placeholders
	c.Rules.datasources = c.Datasources
	c.Rules.databases = c.Databases
	c.Rules.tables = c.Tables
	out, err := yaml.Marshal(raw.Rules)
	if err != nil {
		return err
	}
	err = yaml.Unmarshal(out, &c.Rules)
	if err != nil {
		return err
	}

	if len(c.Rules.Variables) == 0 && !c.testMode {
		return fmt.Errorf("%w: 缺少%s配置", errs.ErrConfigSyntaxInvalid, ConfigSectionTypeRules)
	}

	return nil
}

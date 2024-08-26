package composite

import (
	"fmt"
	"log"

	"github.com/meoying/dbproxy/config/v2/internal/errs"
	"gopkg.in/yaml.v3"
)

type Config struct {
	testMode     bool
	Placeholders *Placeholders      `yaml:"placeholders,omitempty"`
	Datasources  *Datasources       `yaml:"datasources,omitempty"`
	Databases    *Section[Database] `yaml:"databases,omitempty"`
	Tables       *Section[Table]    `yaml:"tables,omitempty"`
	Rules        Rules              `yaml:"rules"`
}

func (c *Config) UnmarshalYAML(value *yaml.Node) error {

	type rawConfig struct {
		Placeholders *Placeholders      `yaml:"placeholders,omitempty"`
		Datasources  *Datasources       `yaml:"datasources,omitempty"`
		Databases    *Section[Database] `yaml:"databases,omitempty"`
		Tables       *Section[Table]    `yaml:"tables,omitempty"`
		Rules        map[string]any     `yaml:"rules"`
	}
	ph := &Placeholders{}
	raw := rawConfig{
		Placeholders: ph,
		Datasources:  &Datasources{globalPlaceholders: ph},
		Databases:    NewSection[Database](ConfigSectionTypeDatabases, nil, ph, NewDatabase),
		Tables:       NewSection[Table](ConfigSectionTypeTables, nil, ph, NewTable),
	}
	err := value.Decode(&raw)
	if err != nil {
		return fmt.Errorf("%w: %w", errs.ErrConfigSyntaxInvalid, err)
	}

	log.Printf("raw.Config = %#v\n", raw)

	// 全局预定义配置
	c.Placeholders = raw.Placeholders
	c.Datasources = raw.Datasources
	c.Databases = raw.Databases
	c.Tables = raw.Tables

	// if !raw.Placeholders.IsZero() {
	// 	c.Placeholders = raw.Placeholders
	// }
	// if !raw.Datasources.IsZero() {
	// 	c.Datasources = raw.Datasources
	// }
	// if !raw.Databases.IsZero() {
	// 	c.Databases = raw.Databases
	// }
	// if !raw.Tables.IsZero() {
	// 	c.Tables = raw.Tables
	// }

	log.Printf("raw.Config.Rules = %#v\n", raw.Rules)
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

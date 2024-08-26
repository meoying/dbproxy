package composite

import (
	"fmt"
	"log"

	"gopkg.in/yaml.v3"
)

type Config struct {
	testMode     bool
	Placeholders *Placeholders `yaml:"placeholders,omitempty"`
	Datasources  *Datasources  `yaml:"datasources,omitempty"`
	Databases    *Databases    `yaml:"databases,omitempty"`
	Tables       *Tables       `yaml:"tables,omitempty"`
	Rules        Rules         `yaml:"rules"`
}

func (c *Config) UnmarshalYAML(value *yaml.Node) error {

	type rawConfig struct {
		Placeholders *Placeholders  `yaml:"placeholders,omitempty"`
		Datasources  *Datasources   `yaml:"datasources,omitempty"`
		Databases    *Databases     `yaml:"databases,omitempty"`
		Tables       *Tables        `yaml:"tables,omitempty"`
		Rules        map[string]any `yaml:"rules"`
	}
	ph := &Placeholders{}
	raw := rawConfig{
		Placeholders: ph,
		Datasources:  &Datasources{globalPlaceholders: ph},
		Databases:    &Databases{globalPlaceholders: ph},
		Tables:       &Tables{globalPlaceholders: ph},
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
		return fmt.Errorf("no rules defined")
	}

	return nil
}

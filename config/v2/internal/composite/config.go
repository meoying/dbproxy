package composite

import (
	"log"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Placeholders *Placeholders  `yaml:"placeholders,omitempty"`
	Datasources  *Datasources   `yaml:"datasources,omitempty"`
	Databases    map[string]any `yaml:"databases,omitempty"`
	Tables       map[string]any `yaml:"tables,omitempty"`
	Rules        Rules          `yaml:"rules"`
}

func (c *Config) UnmarshalYAML(value *yaml.Node) error {

	type rawConfig struct {
		Placeholders *Placeholders  `yaml:"placeholders,omitempty"`
		Datasources  *Datasources   `yaml:"datasources,omitempty"`
		Databases    map[string]any `yaml:"databases,omitempty"`
		Tables       map[string]any `yaml:"tables,omitempty"`
		Rules        map[string]any `yaml:"rules"`
	}
	ph := &Placeholders{}
	raw := rawConfig{
		Placeholders: ph,
		Datasources:  &Datasources{globalPlaceholders: ph},
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
	// c.Rules.databases = c.Placeholders
	// c.Rules.tables = c.Placeholders
	out, err := yaml.Marshal(raw.Rules)
	if err != nil {
		return err
	}
	return yaml.Unmarshal(out, &c.Rules)
}

package composite

import (
	"log"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Datasources  *Datasources   `yaml:"datasources,omitempty"`
	Placeholders *Placeholders  `yaml:"placeholders,omitempty"`
	Databases    map[string]any `yaml:"databases,omitempty"`
	Tables       map[string]any `yaml:"tables,omitempty"`
	Rules        Rules          `yaml:"rules"`
}

func (c *Config) UnmarshalYAML(value *yaml.Node) error {
	type rawConfig struct {
		Datasources  *Datasources   `yaml:"datasources,omitempty"`
		Placeholders *Placeholders  `yaml:"placeholders,omitempty"`
		Databases    map[string]any `yaml:"databases,omitempty"`
		Tables       map[string]any `yaml:"tables,omitempty"`
		Rules        map[string]any `yaml:"rules"`
	}

	var raw rawConfig
	err := value.Decode(&raw)
	if err != nil {
		return err
	}

	log.Printf("raw.Config = %#v\n", raw)

	c.Datasources = raw.Datasources
	c.Placeholders = raw.Placeholders
	c.Databases = raw.Databases
	c.Tables = raw.Tables

	log.Printf("raw.Config.Rules = %#v\n", raw.Rules)
	c.Rules.datasources = c.Datasources
	out, err := yaml.Marshal(raw.Rules)
	if err != nil {
		return err
	}
	return yaml.Unmarshal(out, &c.Rules)
}

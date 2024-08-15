package config

import (
	"log"
	"os"

	"gopkg.in/yaml.v3"
)

type StringType struct {
	Value string `yaml:"string"`
}

type ShardingKey struct {
	Template TemplateType `yaml:"template"`
}

type Database struct {
	StringType   `yaml:"string,omitempty"`
	TemplateType `yaml:"template,omitempty"`
}

type Config struct {
	ShardingKeys map[string]ShardingKey `yaml:"shardingKeys"`
	Databases    map[string]Database    `yaml:"databases"`
}

// ParseFile parses the YAML configuration from a file.
func ParseFile(filePath string) (*Config, error) {
	data, err := os.ReadFile(filePath)
	if err != nil {
		return nil, err
	}
	return parseConfig(data)
}

// ParseContent parses the YAML configuration from file content
func ParseContent(content string) (*Config, error) {
	return parseConfig([]byte(content))
}

// Internal function to parse the YAML data.
func parseConfig(data []byte) (*Config, error) {
	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, err
	}
	log.Printf("parsed config: %#v\n", cfg)
	return &cfg, nil
}

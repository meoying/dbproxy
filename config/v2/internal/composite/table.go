package composite

import (
	"gopkg.in/yaml.v3"
)

type Tables struct {
}

type Table struct {
	Name    string
	varType string
}

func (t *Table) UnmarshalYAML(value *yaml.Node) error {

	type rawTable struct {
		Sharding *Sharding `yaml:"sharding"`
	}
	raw := &rawTable{
		// Sharding: &Sharding{varName: t.varName, config: t.config},
	}
	if err := value.Decode(&raw); err != nil {
		return err
	}

	// t.Sharding = *raw.Sharding
	t.varType = DataTypeSharding
	return nil
}

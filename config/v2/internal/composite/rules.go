package composite

import (
	"fmt"
	"log"

	"github.com/meoying/dbproxy/config/v2/internal/errs"
	"gopkg.in/yaml.v3"
)

type Rules struct {
	placeholders *Section[Placeholder]
	datasources  *Datasources
	databases    *Section[Database]
	tables       *Section[Table]

	Variables map[string]Rule
}

func (r *Rules) UnmarshalYAML(value *yaml.Node) error {
	variables := make(map[string]any)

	err := value.Decode(&variables)
	if err != nil {
		return err
	}

	log.Printf("raw.Rules = %#v\n", variables)
	r.Variables = make(map[string]Rule, len(variables))
	for name, values := range variables {
		v := Rule{
			globalPlaceholders: r.placeholders,
			globalDatasources:  r.datasources,
			globalDatabases:    r.databases,
			globalTables:       r.tables,
		}
		out, err1 := yaml.Marshal(values)
		if err1 != nil {
			return err1
		}
		err1 = yaml.Unmarshal(out, &v)
		if err1 != nil {
			return fmt.Errorf("%w: %w: rules.%s", err1, errs.ErrUnmarshalVariableFailed, name)
		}
		r.Variables[name] = v
	}
	log.Printf("Rules.Variables = %#v\n", r.Variables)
	return nil
}

type Rule struct {
	globalPlaceholders *Section[Placeholder]
	globalDatasources  *Datasources
	globalDatabases    *Section[Database]
	globalTables       *Section[Table]

	Datasources Datasources       `yaml:"datasources"`
	Databases   Section[Database] `yaml:"databases"`
	Tables      Section[Table]    `yaml:"tables"`
}

func (r *Rule) UnmarshalYAML(value *yaml.Node) error {

	type rawRule struct {
		Datasources Datasources        `yaml:"datasources"`
		Databases   *Section[Database] `yaml:"databases"`
		Tables      *Section[Table]    `yaml:"tables"`
	}

	raw := &rawRule{
		Datasources: Datasources{
			globalPlaceholders: r.globalPlaceholders,
			global:             r.globalDatasources,
		},
		Databases: NewSection[Database](ConfigSectionTypeDatabases, r.globalDatabases, r.globalPlaceholders, NewDatabase),
		Tables:    NewSection[Table](ConfigSectionTypeTables, r.globalTables, r.globalPlaceholders, NewTable),
	}
	err := value.Decode(&raw)
	if err != nil {
		return err
	}

	log.Printf("raw.Rule = %#v\n", raw)
	log.Printf("globalDatasources = %#v\n", r.globalDatasources)

	r.Datasources = raw.Datasources
	r.Databases = *raw.Databases
	r.Tables = *raw.Tables
	return nil
}

package composite

import (
	"fmt"
	"log"

	"github.com/meoying/dbproxy/config/v2/internal/errs"
	"gopkg.in/yaml.v3"
)

type Rules struct {
	testMode     bool
	placeholders *Section[Placeholder]
	datasources  *Section[Datasource]
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
			testMode:           r.testMode,
			name:               name,
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
		log.Printf("unmarshal %q = %#v\n rule = %#v\n", name, values, v)
		r.Variables[name] = v
	}
	log.Printf("Rules.Variables = %#v\n", r.Variables)
	return nil
}

type Rule struct {
	testMode           bool
	name               string
	globalPlaceholders *Section[Placeholder]
	globalDatasources  *Section[Datasource]
	globalDatabases    *Section[Database]
	globalTables       *Section[Table]

	Datasources Section[Datasource] `yaml:"datasources"`
	Databases   Section[Database]   `yaml:"databases"`
	Tables      Section[Table]      `yaml:"tables"`
}

func (r *Rule) UnmarshalYAML(value *yaml.Node) error {

	type rawRule struct {
		Datasources *Section[Datasource] `yaml:"datasources"`
		Databases   *Section[Database]   `yaml:"databases"`
		Tables      *Section[Table]      `yaml:"tables"`
	}

	raw := &rawRule{
		Datasources: NewSection[Datasource](ConfigSectionTypeDatasources, r.globalDatasources, r.globalPlaceholders, NewDatasource),
		Databases:   NewSection[Database](ConfigSectionTypeDatabases, r.globalDatabases, r.globalPlaceholders, NewDatabase),
		Tables:      NewSection[Table](ConfigSectionTypeTables, r.globalTables, r.globalPlaceholders, NewTable),
	}
	err := value.Decode(&raw)
	if err != nil {
		return fmt.Errorf("%w: %w", errs.ErrConfigSyntaxInvalid, err)
	}

	log.Printf("raw.Rule = %#v\n", raw)
	log.Printf("globalDatasources = %#v\n", r.globalDatasources)

	if raw.Datasources.IsZero() && !r.testMode {
		return fmt.Errorf("%w: %s缺少%s信息", errs.ErrConfigSyntaxInvalid, r.name, ConfigSectionTypeDatasources)
	}
	r.Databases = *raw.Databases

	if raw.Databases.IsZero() && !r.testMode {
		return fmt.Errorf("%w: %s缺少%s信息", errs.ErrConfigSyntaxInvalid, r.name, ConfigSectionTypeDatabases)
	}
	r.Datasources = *raw.Datasources

	if raw.Tables.IsZero() && !r.testMode {
		return fmt.Errorf("%w: %s缺少%s信息", errs.ErrConfigSyntaxInvalid, r.name, ConfigSectionTypeTables)
	}
	r.Tables = *raw.Tables

	return nil
}

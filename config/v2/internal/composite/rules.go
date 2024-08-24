package composite

import (
	"fmt"
	"log"
	"strings"

	"github.com/meoying/dbproxy/config/v2/internal/errs"
	"gopkg.in/yaml.v3"
)

type Sharding struct {
	Name       string
	Datasource Datasource `yaml:"datasource"`
	Database   Database   `yaml:"database"`
	Table      Variable   `yaml:"table"`
}

func (s *Sharding) UnmarshalYAML(value *yaml.Node) error {
	type rawSharding struct {
		Datasource map[string]any `yaml:"datasource"`
		Database   map[string]any `yaml:"database"`
		Table      any            `yaml:"table"`
	}
	log.Printf("解析前 raw.sharding 前 sharding 自身 = %#v\n", s)
	var raw rawSharding
	if err := value.Decode(&raw); err != nil {
		return err
	}

	log.Printf("解析 raw.Sharding = %#v\n", raw)
	log.Printf("before ds = %#v\n", raw.Datasource)
	log.Printf("before db = %#v\n", raw.Database)
	log.Printf("before tb = %#v\n", raw.Table)

	ds, err := unmarshalShardingFieldVariable[Datasource](ConfigSectionDatasources,
		DataTypeDatasource, s.Name, raw.Datasource)
	if err != nil {
		return err
	}
	s.Datasource = ds
	log.Printf("解析ds成功! = %#v\n", s.Datasource)

	db, err := unmarshalShardingFieldVariable[Database](ConfigFieldDatabases,
		DataTypeDatabase, s.Name, raw.Database)
	if err != nil {
		return err
	}
	s.Database = db
	log.Printf("解析db成功! = %#v\n", s.Database)

	v, err := UnmarshalUntypedVariable(DataTypeVariable, s.Name, raw.Table)
	if err != nil {
		return err
	}
	tb := v.(Variable)
	s.Table = tb
	log.Printf("解析tb成功! = %#v\n", s.Table)
	return nil
}

func unmarshalShardingFieldVariable[T Database | Datasource](fieldType, varType, varName string, variables map[string]any) (T, error) {
	var zero T
	if len(variables) == 0 {
		return zero, fmt.Errorf("%w: %s.sharding.%s", errs.ErrUnmarshalVariableFailed, varName, varType)
	}
	var chosenVarName string
	if p, ok := variables[DataTypeReference]; ok {
		refPath := p.(string)
		if !strings.HasPrefix(refPath, fieldType) {
			return zero, fmt.Errorf("%w: %s", errs.ErrReferencePathInvalid, refPath)
		}
		chosenVarName = refPath
	} else {
		chosenVarName = varName
	}
	v, err := UnmarshalUntypedVariable(varType, chosenVarName, variables)
	if err != nil {
		return zero, err
	}
	return v.(T), nil
}

type Rules struct {
	datasources *Datasources
	variables   map[string]Rule
}

func (r *Rules) UnmarshalYAML(value *yaml.Node) error {
	variables := make(map[string]any)

	err := value.Decode(&variables)
	if err != nil {
		return err
	}

	log.Printf("raw.Rules = %#v\n", variables)
	r.variables = make(map[string]Rule, len(variables))
	for name, values := range variables {
		v := Rule{
			globalDatasources: r.datasources,
		}
		out, err1 := yaml.Marshal(values)
		if err1 != nil {
			return err1
		}
		err1 = yaml.Unmarshal(out, &v)
		if err1 != nil {
			return fmt.Errorf("%w: %w: rules.%s", err1, errs.ErrUnmarshalVariableFailed, name)
		}
		r.variables[name] = v
	}
	log.Printf("Rules.Variables = %#v\n", r.variables)
	return nil
}

type Rule struct {
	globalDatasources *Datasources
	Datasources       Datasources `yaml:"datasources"`
}

func (r *Rule) UnmarshalYAML(value *yaml.Node) error {

	type rawRule struct {
		Datasources Datasources `yaml:"datasources"`
	}

	raw := &rawRule{
		Datasources: Datasources{
			global: r.globalDatasources,
		},
	}
	err := value.Decode(&raw)
	if err != nil {
		return err
	}

	log.Printf("raw.Rule = %#v\n", raw)
	log.Printf("globalDatasources = %#v\n", r.globalDatasources)

	r.Datasources = raw.Datasources
	return nil
}

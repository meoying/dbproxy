package v2

import (
	"fmt"
	"log"
	"reflect"
	"strings"

	"github.com/meoying/dbproxy/config/v2/internal/composite"
	"github.com/meoying/dbproxy/config/v2/internal/errs"
	"gopkg.in/yaml.v3"
)

// Config 配置结构体
type Config struct {
	Variables   map[string]any `yaml:"variables,omitempty"`
	Databases   map[string]any `yaml:"databases,omitempty"`
	Datasources map[string]any `yaml:"datasources,omitempty"`
	Tables      map[string]any `yaml:"tables"`
}

func (c *Config) UnmarshalYAML(value *yaml.Node) error {
	type rawConfig struct {
		Variables   map[string]any `yaml:"variables"`
		Databases   map[string]any `yaml:"databases"`
		Datasources map[string]any `yaml:"datasources"`
		Tables      map[string]any `yaml:"tables"`
	}
	var raw rawConfig
	if err := value.Decode(&raw); err != nil {
		return err
	}

	c.Variables = raw.Variables
	c.Databases = raw.Databases
	c.Datasources = raw.Datasources
	c.Tables = raw.Tables

	// if len(raw.Tables) == 0 {
	// 	return fmt.Errorf("%w: 必备字段: %s, 可选字段: %s", ErrMissingConfigField, ConfigFieldTables,
	// 		strings.Join([]string{ConfigFieldVariables, ConfigFieldDatasources, ConfigFieldDatabases}, ", "))
	// }

	for typ, section := range map[string]map[string]any{
		composite.DataTypeVariable:   c.Variables,
		composite.DataTypeDatabase:   c.Databases,
		composite.DataTypeDatasource: c.Datasources,
		composite.DataTypeTable:      c.Tables,
	} {
		err := Unmarshal(c, typ, section)
		if err != nil {
			return err
		}
	}
	log.Printf("config: %#v\n", c)
	return nil
}

func Unmarshal(c *Config, typ string, variables map[string]any) error {

	log.Printf("Unmarshal typ = %s, variables = %#v\n", typ, variables)
	for name, value := range variables {
		variable, err := UnmarshalUntypedVariable(c, typ, name, value)
		if err != nil {
			return err
		}
		variables[name] = variable
	}

	return nil
}

// UnmarshalUntypedVariable 反序列化未类型化的变量
func UnmarshalUntypedVariable(config *Config, dataType, name string, value any) (any, error) {
	log.Printf("UnmarshalUntypedVariable type = %s, name = %s, value = %#v\n", dataType, name, value)
	var untypedVal map[string]any
	switch val := value.(type) {
	case composite.String, composite.Enum, composite.Hash, composite.Template, composite.Ref, composite.Variable, composite.Database, composite.Datasource, composite.Table:
		return value, nil
	case map[string]any:
		if dataType != "" {
			untypedVal = map[string]any{
				dataType: val,
			}
		} else {
			untypedVal = val
		}
	case []any:
		vv, elemType, err := convertArrayValues(val)
		if err != nil {
			return nil, err
		}
		if dataType != "" {
			untypedVal = map[string]any{
				dataType: map[string]any{
					elemType: vv,
				},
			}
		} else {
			return vv, nil
		}
	case string:
		if dataType != "" {
			untypedVal = map[string]any{
				dataType: map[string]any{
					composite.DataTypeString: val,
				},
			}
		} else {
			return composite.String(val), nil
		}
	}
	log.Printf("UnmarshalUntypedVariable(%s) untyped = %#v\n", name, untypedVal)
	typedVal, err := UnmarshalDataType(config, name, untypedVal)
	if err != nil {
		return nil, err
	}
	log.Printf("UnmarshalUntypedVariable(%s) typed = %#v\n", name, typedVal)
	return typedVal, nil
}

func convertArrayValues(val []any) (any, string, error) {
	switch val[0].(type) {
	case string:
		strs := make(composite.Enum, len(val))
		for i := range val {
			strs[i] = val[i].(string)
		}
		return strs, composite.DataTypeEnum, nil
	default:
		return nil, "unknown", fmt.Errorf("未知的数组元素类型: %t", val[0])
	}
}

func UnmarshalDataType(c *Config, varName string, untypedVal map[string]any) (any, error) {
	dataTypes := map[string]yaml.Unmarshaler{
		// composite.DataTypeTemplate: &composite.Template{
		// 	// Name: varName,
		// },
		// composite.DataTypeReference: &composite.Ref{
		// 	Name: varName,
		// },
		// composite.DataTypeHash: &composite.Hash{Name: varName},

		// composite.DataTypeVariable: &composite.Variable{
		// 	Name: varName,
		// },
		// composite.DataTypeDatabase: &composite.Database{
		// 	Name: varName,
		// },
		// composite.DataTypeDatasource: &composite.Datasource{
		// 	Name: varName,
		// },
		// composite.DataTypeTable: &composite.Table{
		// 	Name: varName,
		// },
		// composite.DataTypeSharding: &composite.Sharding{
		// 	Name: varName,
		// },
	}
	for key, typ := range dataTypes {
		if r, ok := untypedVal[key]; ok {
			err := UnmarshalDataTypeValue(r, typ)
			if err != nil {
				return nil, fmt.Errorf("%w: %q[%s类型]: %w", errs.ErrVariableTypeInvalid, varName, strings.Trim(key, "_"), err)
			}
			return reflect.ValueOf(typ).Elem().Interface(), nil
		}
	}
	return nil, fmt.Errorf("%w: %q", errs.ErrVariableTypeInvalid, varName)
}

func UnmarshalDataTypeValue(rawVal any, typ yaml.Unmarshaler) error {
	log.Printf("rawVal = %#v\n", rawVal)
	node := &yaml.Node{}
	if err := node.Encode(rawVal); err != nil {
		return err
	}
	if err := typ.UnmarshalYAML(node); err != nil {
		return err
	}
	return nil
}

func (c *Config) VariableNames() []string {
	var keys []string
	for k := range c.Variables {
		keys = append(keys, k)
	}
	return keys
}

func (c *Config) VariableByName(name string) (any, error) {
	if v, ok := c.Variables[name]; ok {
		return v, nil
	}
	return nil, fmt.Errorf("%w: %q", errs.ErrVariableNameNotFound, name)
}

func (c *Config) DatasourceNames() []string {
	var keys []string
	for k := range c.Datasources {
		keys = append(keys, k)
	}
	return keys
}

func (c *Config) DatasourceByName(name string) (any, error) {
	if v, ok := c.Datasources[name]; ok {
		return v, nil
	}
	return nil, fmt.Errorf("%w: %q", errs.ErrVariableNameNotFound, name)
}

func (c *Config) DatabaseNames() []string {
	var keys []string
	for k := range c.Databases {
		keys = append(keys, k)
	}
	return keys
}

func (c *Config) DatabaseByName(name string) (any, error) {
	if v, ok := c.Databases[name]; ok {
		return v, nil
	}
	return nil, fmt.Errorf("%w: %q", errs.ErrVariableNameNotFound, name)
}

func (c *Config) TableNames() []string {
	var keys []string
	for k := range c.Tables {
		keys = append(keys, k)
	}
	return keys
}

func (c *Config) TableByName(name string) (any, error) {
	if v, ok := c.Tables[name]; ok {
		return v, nil
	}
	return nil, fmt.Errorf("%w: %q", errs.ErrVariableNameNotFound, name)
}

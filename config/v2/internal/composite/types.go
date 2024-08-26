package composite

import (
	"fmt"
	"log"
	"reflect"
	"strings"

	"github.com/meoying/dbproxy/config/v2/internal/errs"
	"gopkg.in/yaml.v3"
)

const (
	ConfigSectionTypeTables  = "tables"
	ConfigFieldDatabases     = "databases"
	ConfigFieldVariables     = "variables"
	ConfigSectionDatasources = "datasources"
)

const (
	DataTypeString = "string"
	DataTypeEnum   = "enum"
	DataTypeHash   = "hash"

	DataTypeTemplate  = "template"
	DataTypeReference = "ref"

	DataTypeVariable   = "variable"
	DataTypeDatabase   = "database"
	DataTypeDatasource = "datasource"
	DataTypeTable      = "table"
	DataTypeRule       = "rule"
	DataTypeSharding   = "sharding"
)

type (
	Evaluator interface {
		Evaluate() (map[string]string, error)
	}
)

// func isKeyword(name string) bool {
// 	keywords := map[string]struct{}{
// 		"datasources": {},
// 		"databases":   {},
// 		"tables":      {},
// 		"rules":       {},
// 		"ref":         {},
// 		"template":    {},
// 		// "expr":         {},
// 		// "master":       {},
// 		// "slaves":       {},
// 		// "placeholders": {},
// 	}
// 	_, ok := keywords[name]
// 	return ok
// }

func Unmarshal(typ string, variables map[string]any) error {

	log.Printf("Unmarshal typ = %s, variables = %#v\n", typ, variables)
	for name, value := range variables {
		variable, err := UnmarshalUntypedVariable(typ, name, value)
		if err != nil {
			return err
		}
		variables[name] = variable
	}

	return nil
}

// UnmarshalUntypedVariable 反序列化未类型化的变量
func UnmarshalUntypedVariable(dataType, name string, value any) (any, error) {
	log.Printf("UnmarshalUntypedVariable type = %s, name = %s, value = %#v\n", dataType, name, value)
	var untypedVal map[string]any
	switch val := value.(type) {
	case String, Enum, Hash, Template, Ref, Variable, Database, Datasource, Table:
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
					DataTypeString: val,
				},
			}
		} else {
			return String(val), nil
		}
	}
	log.Printf("UnmarshalUntypedVariable(%s) untyped = %#v\n", name, untypedVal)
	typedVal, err := UnmarshalVariable(name, untypedVal)
	if err != nil {
		return nil, err
	}
	log.Printf("UnmarshalUntypedVariable(%s) typed = %#v\n", name, typedVal)
	return typedVal, nil
}

func convertArrayValues(val []any) (any, string, error) {
	switch val[0].(type) {
	case string:
		strs := make(Enum, len(val))
		for i := range val {
			strs[i] = val[i].(string)
		}
		return strs, DataTypeEnum, nil
	default:
		return nil, "unknown", fmt.Errorf("未知的数组元素类型: %t", val[0])
	}
}

func UnmarshalVariable(Name string, values map[string]any) (any, error) {
	dataTypes := map[string]yaml.Unmarshaler{
		// DataTypeDatasource: &Datasource{
		// 	// Name: Name,
		// },

		DataTypeTemplate: &Template{
			// Name: Name,
		},
		DataTypeReference: &Ref{
			Name: Name,
		},

		// DataTypeHash: &Hash{Name: varName},

		DataTypeVariable: &Variable{
			Name: Name,
		},
		// DataTypeDatabase: &Database{
		// 	// Name: Name,
		// },

		// DataTypeTable: &Table{
		// 	Name: Name,
		// },
		DataTypeSharding: &Sharding{
			Name: Name,
		},
	}
	for typeName, typ := range dataTypes {
		if val, ok := values[typeName]; ok {
			err := UnmarshalTypedValue(typ, val)
			if err != nil {
				return nil, fmt.Errorf("%w: %q[%s类型]: %w", errs.ErrVariableTypeInvalid, Name, strings.Trim(typeName, "_"), err)
			}
			return reflect.ValueOf(typ).Elem().Interface(), nil
		}
	}
	return nil, fmt.Errorf("%w: %q", errs.ErrVariableTypeInvalid, Name)
}

func UnmarshalTypedValue(typ yaml.Unmarshaler, rawVal any) error {
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

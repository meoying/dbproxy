package composite

import (
	"fmt"
	"log"

	"github.com/meoying/dbproxy/config/v2/internal/errs"
	"gopkg.in/yaml.v3"
)

type Databases struct {
	global             *Databases
	globalPlaceholders *Placeholders
	Variables          map[string]Database
}

func (d *Databases) Type() string {
	return "databases"
}

func (d *Databases) Find(name string) (Database, error) {
	v, ok := d.Variables[name]
	if !ok {
		return Database{}, fmt.Errorf("%w: %s", errs.ErrVariableNameNotFound, name)
	}
	return v, nil
}

func (d *Databases) IsZero() bool {
	return len(d.Variables) == 0
}

func (d *Databases) isGlobal() bool {
	return d.global == nil
}

func (d *Databases) UnmarshalYAML(value *yaml.Node) error {
	// 尝试解析为 string
	var stringData string
	if err := value.Decode(&stringData); err == nil {
		// 成功解析为 string
		return d.unmarshalMapVariables(map[string]any{
			"": stringData,
		})
	}

	// 尝试解析为 []any
	var sliceData []any
	if err := value.Decode(&sliceData); err == nil {
		return d.unmarshalMapVariables(map[string]any{
			"": sliceData,
		})
	}

	// 尝试解析为 map[string]interface{}
	var mapData map[string]any
	if err := value.Decode(&mapData); err == nil {
		// 成功解析为 map
		return d.unmarshalMapVariables(mapData)
	}

	// 如果都不是，返回错误
	return fmt.Errorf("%w: databases", errs.ErrUnmarshalVariableFailed)
}

func (d *Databases) unmarshalMapVariables(variables map[string]any) error {
	log.Printf("raw.databases >>>  = %#v\n", variables)
	d.Variables = make(map[string]Database, len(variables))
	for name, val := range variables {
		if !d.isGlobal() {
			// 在局部datasources中引用
			if name == DataTypeReference {
				val = map[string]any{
					DataTypeReference: val,
				}
			}
		}

		v, err := unmarshal[Database, *Databases](d.globalPlaceholders, val)
		if err != nil {
			return fmt.Errorf("%w: %w: %s.%s", err, errs.ErrUnmarshalVariableFailed, d.Type(), name)
		}
		ref, ok := v.(Reference[Database, *Databases])
		if ok {
			ref.global = d.global
			refVars, err1 := ref.Build()
			if err1 != nil {
				return err1
			}
			for n, v := range refVars {
				if n == "" {
					n = name
				}
				d.Variables[n] = v
			}
		} else {
			d.Variables[name] = Database{Value: v}
		}
	}
	return nil
}

// Database 数据库类型
type Database struct {
	// Name string
	// varType string
	Value any
}

func NewDatabase(v any) Database {
	return Database{Value: v}
}

// func (d *Database) UnmarshalYAML(value *yaml.Node) error {
// 	value.Decode(&d.Value)
// }
// func (d *Database) UnmarshalYAML(value *yaml.Node) error {
// 	type rawDatabase struct {
// 		Tmpl *Template `yaml:"template,omitempty"`
// 		Str  string    `yaml:"string,omitempty"`
// 		Ref  *Ref      `yaml:"ref,omitempty"`
// 	}
// 	raw := &rawDatabase{
// 		Tmpl: &Template{},
// 		Ref:  &Ref{Name: d.Name},
// 	}
// 	if err := value.Decode(raw); err != nil {
// 		return err
// 	}
// 	log.Printf("raw.Database = %#v\n", raw)
//
// 	if raw.Str == "" && raw.Tmpl.IsZero() && raw.Ref.IsZero() {
// 		return fmt.Errorf("%w: %s.%q", errs.ErrUnmarshalVariableFailed, ConfigFieldDatabases, d.Name)
// 	}
//
// 	if raw.Str != "" {
// 		d.varType = DataTypeString
// 		d.Value = NewAny(String(raw.Str))
// 	}
//
// 	if !raw.Tmpl.IsZero() {
// 		tmpl := *raw.Tmpl
// 		// tmpl.Name = d.Name
// 		d.varType = DataTypeTemplate
// 		d.Value = NewAny(&tmpl)
// 	}
//
// 	if !raw.Ref.IsZero() {
// 		log.Printf("Database 中 解析到的 ref = %#v\n", raw.Ref)
// 		d.varType = raw.Ref.varType
// 		d.Value = raw.Ref.Values
// 	}
//
// 	return nil
// }

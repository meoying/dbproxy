package v2

import (
	"fmt"
	"log"
	"strconv"

	"github.com/ecodeclub/ekit/slice"
	"gopkg.in/yaml.v3"
)

const (
	ConfigSectionTypePlaceholders = "placeholders"
	ConfigSectionTypeDatasources  = "datasources"
	ConfigSectionTypeDatabases    = "databases"
	ConfigSectionTypeTables       = "tables"
	ConfigSectionTypeRules        = "rules"
)

const (
	DataTypePlaceholder        = "placeholder"
	DataTypeDatasource         = "datasource"
	DataTypeDatabase           = "database"
	DataTypeTable              = "table"
	DataTypeTemplate           = "template"
	DataTypeDatasourceTemplate = "ds_template"
	DataTypeReference          = "ref"
)

type (
	Evaluable interface {
		Evaluate() (map[string]string, error)
	}
)

// Placeholder 占位符类型
type Placeholder struct {
	Value any
}

func NewPlaceholder(value any) Placeholder {
	return Placeholder{
		Value: value,
	}
}

func (p Placeholder) TypeName() string {
	return DataTypePlaceholder
}

func (p Placeholder) Evaluator() Evaluable {
	switch v := p.Value.(type) {
	case String:
		return v
	case Enum:
		return v
	case Hash:
		return &v
	default:
		return nil
	}
}

// Database 数据库类型
type Database struct {
	Value any
}

func NewDatabase(v any) Database {
	return Database{Value: v}
}

func (d Database) TypeName() string {
	return DataTypeDatabase
}

// Table 数据表类型
type Table struct {
	Value any
}

func NewTable(value any) Table {
	return Table{Value: value}
}

func (t Table) TypeName() string {
	return DataTypeTable
}

// AnyValue 用于存储反序列化后的多种类型的值
type AnyValue[E Referencable, F Finder[E]] struct {
	Hash     Hash      `yaml:"hash,omitempty"`
	Template *Template `yaml:"template,omitempty"`

	// 引用类型
	Ref *Reference[E, F] `yaml:"ref,omitempty"`

	// datasources 下的值
	Master     String              `yaml:"master"`
	Slaves     Enum                `yaml:"slaves,omitempty"`
	DSTemplate *DatasourceTemplate `yaml:"ds_template,omitempty"`
}

func NewAnyValue[E Referencable, F Finder[E]](ph *Section[Placeholder]) *AnyValue[E, F] {
	return &AnyValue[E, F]{
		Template:   &Template{global: ph},
		Ref:        &Reference[E, F]{},
		DSTemplate: &DatasourceTemplate{global: ph},
	}
}

func unmarshal[E Referencable, F Finder[E]](ph *Section[Placeholder], value any) (any, error) {
	log.Printf("unmarshal value = %#v\n", value)
	switch v := value.(type) {
	case int:
		return String(strconv.Itoa(v)), nil
	case string:
		return String(v), nil
	case []any:
		return convert(v)
	case map[string]any:
		out, err1 := yaml.Marshal(modifyDatasourceTemplateName[E](v))
		if err1 != nil {
			return nil, err1
		}
		a := NewAnyValue[E, F](ph)
		err1 = yaml.Unmarshal(out, a)
		if err1 != nil {
			return nil, err1

		} else if !a.Hash.IsZero() {
			log.Printf("hash value = %#v\n", v)
			return a.Hash, nil
		} else if !a.Template.IsZero() {
			log.Printf("template value = %#v\n tmpl = %#v\n", v, *a.Template)
			return *a.Template, nil

		} else if !a.Ref.IsZero() {

			if !a.DSTemplate.IsZero() {
				// TODO: 写测试覆盖该分支
				// datasources 中 ref 不能与template同时出现
				return nil, fmt.Errorf("%w: ref不能与template并用", ErrVariableTypeInvalid)
			}

			return *a.Ref, nil
		} else if !a.Master.IsZero() {

			masterSlaves := MasterSlaves{
				Master: a.Master,
				Slaves: a.Slaves,
			}

			if !a.Ref.IsZero() {
				// TODO: 写测试覆盖该分支
				return nil, fmt.Errorf("%w: master、salves不能与ref并用", ErrVariableTypeInvalid)
			}

			if !a.DSTemplate.IsZero() {
				// TODO: 写测试覆盖该分支
				return nil, fmt.Errorf("%w: master、salves不能与template并用", ErrVariableTypeInvalid)
			}

			log.Printf("datasources masterSlaves value = %#v\n", masterSlaves)
			return masterSlaves, nil
		} else if !a.DSTemplate.IsZero() {

			if !a.Ref.IsZero() {
				// TODO: 写测试覆盖该分支
				// datasources 中 ref 不能与template同时出现
				return nil, fmt.Errorf("%w: template不能与ref并用", ErrVariableTypeInvalid)
			}

			log.Printf("datasources template value = %#v\n tmpl = %#v\n", v, *a.DSTemplate)
			return *a.DSTemplate, nil
		} else {
			return nil, fmt.Errorf("%w", ErrVariableTypeInvalid)
		}
	default:
		return nil, fmt.Errorf("%w", ErrVariableTypeInvalid)
	}
}

func modifyDatasourceTemplateName[E Referencable](value map[string]any) map[string]any {
	var e E
	if e.TypeName() != DataTypeDatasource {
		return value
	}
	mp := make(map[string]any, len(value))
	for name, val := range value {
		if name == DataTypeTemplate {
			name = DataTypeDatasourceTemplate
		}
		mp[name] = val
	}
	return mp
}

func convert(values []any) (any, error) {
	if len(values) == 0 {
		return nil, fmt.Errorf("%w", ErrVariableTypeInvalid)
	}
	switch values[0].(type) {
	case int, string:
		return Enum(slice.Map(values, func(idx int, src any) string {
			switch v := src.(type) {
			case int:
				return strconv.Itoa(v)
			default:
				return src.(string)
			}
		})), nil
	case map[string]any:
		res := make([]MasterSlaves, len(values))
		for i, val := range values {
			out, err := yaml.Marshal(val)
			if err != nil {
				return nil, err
			}
			err = yaml.Unmarshal(out, &res[i])
			if err != nil {
				return nil, err
			}
		}
		return res, nil
	default:
		return nil, fmt.Errorf("%w", ErrVariableTypeInvalid)
	}
}

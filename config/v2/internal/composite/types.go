package composite

import (
	"fmt"
	"log"
	"strconv"

	"github.com/ecodeclub/ekit/slice"
	"github.com/meoying/dbproxy/config/v2/internal/errs"
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
	DataTypeTemplate  = "template"
	DataTypeReference = "ref"
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

func NewPlaceholderV1(value any) Placeholder {
	return Placeholder{
		Value: value,
	}
}

// Database 数据库类型
type Database struct {
	Value any
}

func NewDatabase(v any) Database {
	return Database{Value: v}
}

// Table 数据表类型
type Table struct {
	Value any
}

func NewTable(value any) Table {
	return Table{Value: value}
}

type Composite[E Referencable, F Finder[E]] struct {
	Hash     Hash      `yaml:"hash,omitempty"`
	Template *Template `yaml:"template,omitempty"`
	// Placeholders *Section[Placeholder] `yaml:"placeholders,omitempty"`
	Ref *Reference[E, F] `yaml:"ref,omitempty"`

	Master     String              `yaml:"master"`
	Slaves     Enum                `yaml:"slaves,omitempty"`
	DSTemplate *DatasourceTemplate `yaml:"ds_template,omitempty"`
}

func NewComposite[E Referencable, F Finder[E]](ph *Section[Placeholder]) *Composite[E, F] {
	return &Composite[E, F]{
		Template:   &Template{global: ph},
		Ref:        &Reference[E, F]{},
		DSTemplate: &DatasourceTemplate{global: ph},
		// Placeholders: NewSection[Placeholder](ConfigSectionTypePlaceholders, ph, nil, NewPlaceholderV1),
	}
}

func unmarshal[E Referencable, F Finder[E]](ph *Section[Placeholder], val any) (any, error) {
	switch v := val.(type) {
	case int:
		return String(strconv.Itoa(v)), nil
	case string:
		return String(v), nil
	case []any:
		return Enum(slice.Map(v, func(idx int, src any) string {
			switch v := src.(type) {
			case int:
				return strconv.Itoa(v)
			default:
				return src.(string)
			}
		})), nil
	case map[string]any:
		out, err1 := yaml.Marshal(v)
		if err1 != nil {
			return nil, err1
		}
		t := NewComposite[E, F](ph)
		err1 = yaml.Unmarshal(out, t)
		if err1 != nil {
			return nil, err1

		} else if !t.Hash.IsZero() {
			log.Printf("hash value = %#v\n", v)
			return t.Hash, nil
		} else if !t.Template.IsZero() {
			log.Printf("template value = %#v\n tmpl = %#v\n", v, *t.Template)
			return *t.Template, nil
		} else if !t.DSTemplate.IsZero() {
			log.Printf("datasources template value = %#v\n tmpl = %#v\n", v, *t.DSTemplate)
			return *t.DSTemplate, nil
		} else if !t.Ref.IsZero() {
			return *t.Ref, nil
		} else {
			return nil, fmt.Errorf("%w", errs.ErrVariableTypeInvalid)
		}
	default:
		return nil, fmt.Errorf("%w", errs.ErrVariableTypeInvalid)
	}
}

package composite

import (
	"fmt"
	"log"

	"github.com/ecodeclub/ekit/slice"
	"github.com/meoying/dbproxy/config/v2/internal/errs"
	"gopkg.in/yaml.v3"
)

const (
	ConfigSectionTypeDatasources = "datasources"
	ConfigSectionTypeDatabases   = "databases"
	ConfigSectionTypeTables      = "tables"
	ConfigSectionTypeRules       = "rules"
)

const (
	DataTypeTemplate  = "template"
	DataTypeReference = "ref"
)

type (
	Evaluator interface {
		Evaluate() (map[string]string, error)
	}
)

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
	Hash Hash      `yaml:"hash,omitempty"`
	Tmpl *Template `yaml:"template,omitempty"`
	Ref  *Reference[E, F]
}

func NewComposite[E Referencable, F Finder[E]](ph *Placeholders) *Composite[E, F] {
	return &Composite[E, F]{
		Tmpl: NewTemplate(ph),
		Ref:  &Reference[E, F]{},
	}
}

func unmarshal[E Referencable, F Finder[E]](ph *Placeholders, val any) (any, error) {
	switch v := val.(type) {
	case string:
		return String(v), nil
	case []any:
		return Enum(slice.Map(v, func(idx int, src any) string {
			return src.(string)
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
		} else if !t.Tmpl.IsZero() {
			log.Printf("template value = %#v\n tmpl = %#v\n", v, *t.Tmpl)
			return *t.Tmpl, nil
		} else if !t.Ref.IsZero() {
			return *t.Ref, nil
		} else {
			return nil, fmt.Errorf("%w", errs.ErrVariableTypeInvalid)
		}
	default:
		return nil, fmt.Errorf("%w", errs.ErrVariableTypeInvalid)
	}
}

package composite

import (
	"fmt"
	"log"

	"github.com/ecodeclub/ekit/slice"
	"github.com/meoying/dbproxy/config/v2/internal/errs"
	"gopkg.in/yaml.v3"
)

type Placeholders struct {
	// global    *Placeholders
	variables map[string]Placeholder
}

func (p *Placeholders) IsZeroValue() bool {
	return len(p.variables) == 0
}

func (p *Placeholders) UnmarshalYAML(value *yaml.Node) error {
	variables := make(map[string]any)
	err := value.Decode(&variables)
	if err != nil {
		return err
	}
	log.Printf("raw.Placeholders.Variables = %#v\n", variables)
	p.variables = make(map[string]Placeholder, len(variables))
	for name, val := range variables {
		ph := Placeholder{}
		switch v := val.(type) {
		case string:
			ph.String = String(v)
		case []any:
			// 引用类型, 非全局Placeholders,中变量可以引用全局Placeholders
			ph.Enum = slice.Map(v, func(idx int, src any) string {
				return src.(string)
			})
		case map[string]any:
			log.Printf("hash value = %#v\n", v)
			var h struct {
				Hash Hash `yaml:"hash,omitempty"`
			}
			out, err1 := yaml.Marshal(v)
			if err1 != nil {
				return err1
			}
			err1 = yaml.Unmarshal(out, &h)
			if err1 != nil || h.Hash.IsZeroValue() {
				return fmt.Errorf("%w: 复合类型当前仅支持哈希: %s", errs.ErrVariableTypeInvalid, err1)
			}
			ph.Hash = h.Hash
		default:
			return fmt.Errorf("%w: %q", errs.ErrVariableTypeInvalid, v)
		}
		p.variables[name] = ph
	}
	log.Printf("Placeholders = %#v\n", p)
	return nil
}

type Placeholder struct {
	Enum   Enum
	String String
	Hash   Hash
}

func (p *Placeholder) Value() Evaluator {
	if len(p.Enum) > 0 {
		return p.Enum
	} else if p.String != "" {
		return p.String
	} else if !p.Hash.IsZeroValue() {
		return &p.Hash
	}
	return nil
}

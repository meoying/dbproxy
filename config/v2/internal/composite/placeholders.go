package composite

import (
	"fmt"
	"log"
	"strconv"

	"github.com/ecodeclub/ekit/slice"
	"github.com/meoying/dbproxy/config/v2/internal/errs"
	"gopkg.in/yaml.v3"
)

type Placeholders struct {
	global    *Placeholders
	Variables map[string]Placeholder
}

func (p *Placeholders) Type() string {
	return "placeholders"
}

func (p *Placeholders) Find(name string) (Placeholder, error) {
	ph, ok := p.Variables[name]
	if !ok {
		return Placeholder{}, fmt.Errorf("%w: %s", errs.ErrVariableNameNotFound, name)
	}
	return ph, nil
}

func (p *Placeholders) IsZero() bool {
	return len(p.Variables) == 0
}

func (d *Placeholders) isGlobal() bool {
	return d.global == nil
}

func (p *Placeholders) UnmarshalYAML(value *yaml.Node) error {
	variables := make(map[string]any)
	err := value.Decode(&variables)
	if err != nil {
		return err
	}

	log.Printf("raw.Placeholders.Variables = %#v\n", variables)
	p.Variables = make(map[string]Placeholder, len(variables))
	for name, val := range variables {

		if !p.isGlobal() {
			// 在局部datasources中引用
			if name == DataTypeReference {
				val = map[string]any{
					DataTypeReference: val,
				}
			}
		}

		ph := Placeholder{}
		switch v := val.(type) {
		case int:
			ph.String = String(strconv.Itoa(v))
			p.Variables[name] = ph
		case string:
			ph.String = String(v)
			p.Variables[name] = ph
		case []any:
			ph.Enum = slice.Map(v, func(idx int, src any) string {
				switch t := src.(type) {
				case int:
					return strconv.Itoa(t)
				default:
					return src.(string)
				}
			})
			p.Variables[name] = ph
		case map[string]any:
			var h struct {
				Hash Hash `yaml:"hash,omitempty"`
				Ref  Reference[Placeholder, *Placeholders]
			}
			out, err1 := yaml.Marshal(v)
			if err1 != nil {
				return err1
			}
			err1 = yaml.Unmarshal(out, &h)
			if err1 != nil {
				return fmt.Errorf("%w: %w: placeholders.%s", err1, errs.ErrVariableTypeInvalid, name)
			} else if !h.Hash.IsZero() {
				log.Printf("hash value = %#v\n", v)
				ph.Hash = h.Hash
				p.Variables[name] = ph
			} else if !p.isGlobal() && !h.Ref.IsZero() {
				h.Ref.global = p.global
				log.Printf("ref value = %#v\n", v)
				build, err2 := h.Ref.Build()
				if err2 != nil {
					return err2
				}
				for n, ph := range build {
					if name != "" && name != "ref" {
						p.Variables[name] = ph
						continue
					}
					p.Variables[n] = ph
				}
			} else {
				return fmt.Errorf("%w: %w: placeholders.%s", err1, errs.ErrVariableTypeInvalid, name)
			}
		default:
			return fmt.Errorf("%w: %q", errs.ErrVariableTypeInvalid, v)
		}

	}
	log.Printf("解析后的 Placeholders = %#v\n", p)
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
	} else if !p.Hash.IsZero() {
		return &p.Hash
	}
	return nil
}

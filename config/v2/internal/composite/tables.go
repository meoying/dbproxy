package composite

import (
	"fmt"
	"log"

	"github.com/meoying/dbproxy/config/v2/internal/errs"
	"gopkg.in/yaml.v3"
)

type Tables struct {
	global             *Tables
	globalPlaceholders *Placeholders
	Variables          map[string]Table
}

func (t *Tables) Type() string {
	return "tables"
}

func (t *Tables) Find(name string) (Table, error) {
	v, ok := t.Variables[name]
	if !ok {
		return Table{}, fmt.Errorf("%w: %s", errs.ErrVariableNameNotFound, name)
	}
	return v, nil
}

func (t *Tables) IsZero() bool {
	return len(t.Variables) == 0
}

func (t *Tables) isGlobal() bool {
	return t.global == nil
}

func (t *Tables) UnmarshalYAML(value *yaml.Node) error {
	// 尝试解析为 string
	var stringData string
	if err := value.Decode(&stringData); err == nil {
		// 成功解析为 string
		return t.unmarshalMapVariables(map[string]any{
			"": stringData,
		})
	}

	// 尝试解析为 []any
	var sliceData []any
	if err := value.Decode(&sliceData); err == nil {
		return t.unmarshalMapVariables(map[string]any{
			"": sliceData,
		})
	}

	// 尝试解析为 map[string]interface{}
	var mapData map[string]any
	if err := value.Decode(&mapData); err == nil {
		// 成功解析为 map
		return t.unmarshalMapVariables(mapData)
	}

	// 如果都不是，返回错误
	return fmt.Errorf("%w: databases", errs.ErrUnmarshalVariableFailed)
}

func (t *Tables) unmarshalMapVariables(variables map[string]any) error {
	log.Printf("raw.databases >>>  = %#v\n", variables)
	t.Variables = make(map[string]Table, len(variables))
	for name, val := range variables {

		if !t.isGlobal() {
			// 在局部datasources中引用
			if name == DataTypeReference {
				val = map[string]any{
					DataTypeReference: val,
				}
			}
		}

		v, err1 := unmarshal[Table, *Tables](t.globalPlaceholders, val)
		if err1 != nil {
			return fmt.Errorf("%w: %w: %s.%s", err1, errs.ErrUnmarshalVariableFailed, t.Type(), name)
		}

		ref, ok := v.(Reference[Table, *Tables])
		if ok {
			ref.global = t.global
			refVars, err1 := ref.Build()
			if err1 != nil {
				return err1
			}
			for n, v := range refVars {
				if n == "" {
					n = name
				}
				t.Variables[n] = v
			}
		} else {
			t.Variables[name] = Table{Value: v}
		}
	}
	return nil
}

// type Tables struct {
//
// }

type Table struct {
	Value any
}

func NewTable(value any) Table {
	return Table{Value: value}
}

type TablesV1 struct {
	Variables map[string]Table
}

func (t *TablesV1) Type() string {
	return "tables"
}

func (t *TablesV1) Find(name string) (Table, error) {
	v, ok := t.Variables[name]
	if !ok {
		return Table{}, fmt.Errorf("%w: %s", errs.ErrVariableNameNotFound, name)
	}
	return v, nil
}

type Creator[T Referencable] func(value any) T

type Section[E Referencable] struct {
	typeName           string
	global             *Section[E]
	globalPlaceholders *Placeholders
	creator            Creator[E]
	Variables          map[string]E
}

func NewSection[E Referencable](typ string, global *Section[E], ph *Placeholders, creator Creator[E]) *Section[E] {
	return &Section[E]{
		typeName:           typ,
		global:             global,
		globalPlaceholders: ph,
		creator:            creator,
		Variables:          make(map[string]E),
	}
}

func (s *Section[E]) Type() string {
	return s.typeName
}

func (s *Section[E]) Find(name string) (E, error) {
	var zero E
	v, ok := s.Variables[name]
	if !ok {
		return zero, fmt.Errorf("%w: %s", errs.ErrVariableNameNotFound, name)
	}
	return v, nil
}

func (s *Section[E]) isGlobal() bool {
	return s.global == nil
}

func (s *Section[E]) UnmarshalYAML(value *yaml.Node) error {
	// 尝试解析为 string
	var stringData string
	if err := value.Decode(&stringData); err == nil {
		// 成功解析为 string
		return s.unmarshalMapVariables(map[string]any{
			"": stringData,
		})
	}

	// 尝试解析为 []any
	var sliceData []any
	if err := value.Decode(&sliceData); err == nil {
		return s.unmarshalMapVariables(map[string]any{
			"": sliceData,
		})
	}

	// 尝试解析为 map[string]interface{}
	var mapData map[string]any
	if err := value.Decode(&mapData); err == nil {
		// 成功解析为 map
		return s.unmarshalMapVariables(mapData)
	}

	// 如果都不是，返回错误
	return fmt.Errorf("%w: %s", errs.ErrUnmarshalVariableFailed, s.typeName)
}

func (s *Section[E]) unmarshalMapVariables(variables map[string]any) error {
	log.Printf("raw.databases >>>  = %#v\n", variables)
	s.Variables = make(map[string]E, len(variables))
	for name, val := range variables {

		if !s.isGlobal() {
			// 在局部datasources中引用
			if name == DataTypeReference {
				val = map[string]any{
					DataTypeReference: val,
				}
			}
		}

		v, err1 := unmarshal[E, *Section[E]](s.globalPlaceholders, val)
		if err1 != nil {
			return fmt.Errorf("%w: %w: %s.%s", err1, errs.ErrUnmarshalVariableFailed, "Section", name)
		}

		ref, ok := v.(Reference[E, *Section[E]])
		if ok {
			ref.global = s.global
			refVars, err1 := ref.Build()
			if err1 != nil {
				return err1
			}
			for n, v := range refVars {
				if n == "" {
					n = name
				}
				s.Variables[n] = v
			}
		} else {
			s.Variables[name] = s.creator(v)
		}
	}
	return nil
}

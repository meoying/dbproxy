package composite

import (
	"fmt"
	"log"

	"github.com/meoying/dbproxy/config/v2/internal/errs"
	"gopkg.in/yaml.v3"
)

type Creator[T Referencable] func(value any) T

type Section[E Referencable] struct {
	typeName           string
	global             *Section[E]
	globalPlaceholders *Section[Placeholder]
	creator            Creator[E]
	Variables          map[string]E
}

func NewSection[E Referencable](typ string, global *Section[E], ph *Section[Placeholder], creator Creator[E]) *Section[E] {
	return &Section[E]{
		typeName:           typ,
		global:             global,
		globalPlaceholders: ph,
		creator:            creator,
		// Variables:          make(map[string]E),
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

func (s *Section[E]) IsZero() bool {
	log.Printf("isZero: %#v\n", s)
	return len(s.Variables) == 0
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
	log.Printf("raw.Section[E] >>>  = %#v\n", variables)
	s.Variables = make(map[string]E, len(variables))
	for name, val := range variables {

		if !s.isGlobal() {
			// 在局部datasources中引用
			if name == DataTypeReference {
				val = map[string]any{
					DataTypeReference: val,
				}
			} else if name == DataTypeTemplate {
				val = map[string]any{
					DataTypeTemplate: val,
				}
			}
		}

		v, err1 := unmarshal[E, *Section[E]](s.globalPlaceholders, val)
		if err1 != nil {
			return fmt.Errorf("%w: %w: %s.%s", err1, errs.ErrUnmarshalVariableFailed, s.typeName, name)
		}

		if ref, ok := v.(Reference[E, *Section[E]]); ok {
			if s.isGlobal() && ref.IsSection(s.typeName) {
				return fmt.Errorf("%w: %s: 不支持引用%s内变量", errs.ErrVariableTypeInvalid, name, s.typeName)
			}
			ref.global = s.global
			refVars, err1 := ref.Build()
			if err1 != nil {
				return err1
			}
			for n, v := range refVars {
				// TODO: 多个引用的时候会会出问题
				if n == "" || (name != DataTypeReference && name != DataTypeTemplate) {
					n = name
				}

				s.Variables[n] = v
			}
		} else if _, ok = v.(Template); ok && s.typeName == ConfigSectionTypePlaceholders {
			return fmt.Errorf("%w: %s: %s内不支持模版类型", errs.ErrVariableTypeInvalid, name, s.typeName)
		} else {
			s.Variables[name] = s.creator(v)
		}
	}
	return nil
}

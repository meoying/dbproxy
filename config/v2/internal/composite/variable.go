package composite

import (
	"fmt"
	"log"

	"github.com/meoying/dbproxy/config/v2/internal/errs"
	"gopkg.in/yaml.v3"
)

// Variable 变量类型
type Variable struct {
	Name    string
	varType string
	Value   any
}

func (v *Variable) UnmarshalYAML(value *yaml.Node) error {
	type rawVariable struct {
		Str  string    `yaml:"string,omitempty"`
		Enum []string  `yaml:"enum,omitempty"`
		Hash *Hash     `yaml:"hash,omitempty"`
		Tmpl *Template `yaml:"template,omitempty"`
		Ref  *Ref      `yaml:"ref,omitempty"`
	}

	raw := &rawVariable{
		Tmpl: &Template{},
		Ref:  &Ref{Name: v.Name},
		Hash: &Hash{},
	}
	if err := value.Decode(raw); err != nil {
		return err
	}

	log.Printf("raw.Variables = %#v\n", raw)

	if raw.Str == "" && len(raw.Enum) == 0 &&
		raw.Hash.IsZero() && raw.Tmpl.IsZero() && raw.Ref.IsZeroValue() {
		return fmt.Errorf("%w: variables.%q", errs.ErrUnmarshalVariableFailed, v.Name)
	}

	if raw.Str != "" {
		v.varType = DataTypeString
		v.Value = String(raw.Str)
	} else if len(raw.Enum) > 0 {
		v.varType = DataTypeEnum
		v.Value = Enum(raw.Enum)
	} else if !raw.Hash.IsZero() {
		hash := *raw.Hash
		// hash.Name = v.Name
		v.varType = DataTypeHash
		v.Value = hash
	} else if !raw.Tmpl.IsZero() {
		tmpl := *raw.Tmpl
		// tmpl.Name = v.Name
		v.varType = DataTypeTemplate
		v.Value = tmpl
	} else if !raw.Ref.IsZeroValue() {
		log.Printf("Variable 中 解析到的 ref = %#v\n", raw.Ref)
		v.varType = raw.Ref.varType
		v.Value = raw.Ref.Values
	}
	return nil
}

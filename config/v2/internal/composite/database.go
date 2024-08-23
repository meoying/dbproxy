package composite

import (
	"fmt"
	"log"

	"github.com/meoying/dbproxy/config/v2/internal/errs"
	"gopkg.in/yaml.v3"
)

type Databases struct {
}

// Database 数据库类型
type Database struct {
	Name    string
	varType string
	Value   any
}

func (d *Database) UnmarshalYAML(value *yaml.Node) error {
	type rawDatabase struct {
		Tmpl *Template `yaml:"template,omitempty"`
		Str  string    `yaml:"string,omitempty"`
		Ref  *Ref      `yaml:"ref,omitempty"`
	}
	raw := &rawDatabase{
		Tmpl: &Template{},
		Ref:  &Ref{Name: d.Name},
	}
	if err := value.Decode(raw); err != nil {
		return err
	}
	log.Printf("raw.Database = %#v\n", raw)

	if raw.Str == "" && raw.Tmpl.IsZeroValue() && raw.Ref.IsZeroValue() {
		return fmt.Errorf("%w: %s.%q", errs.ErrUnmarshalVariableFailed, ConfigFieldDatabases, d.Name)
	}

	if raw.Str != "" {
		d.varType = DataTypeString
		d.Value = String(raw.Str)
	}

	if !raw.Tmpl.IsZeroValue() {
		tmpl := *raw.Tmpl
		// tmpl.Name = d.Name
		d.varType = DataTypeTemplate
		d.Value = tmpl
	}

	if !raw.Ref.IsZeroValue() {
		log.Printf("Database 中 解析到的 ref = %#v\n", raw.Ref)
		d.varType = raw.Ref.varType
		d.Value = raw.Ref.Values
	}

	return nil
}

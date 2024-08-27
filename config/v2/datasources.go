package v2

import (
	"log"

	"gopkg.in/yaml.v3"
)

// Datasource 数据源类型
type Datasource struct {
	Value any
}

func (d Datasource) TypeName() string {
	return DataTypeDatasource
}

func NewDatasource(value any) Datasource {
	return Datasource{
		Value: value,
	}
}

type DatasourceSectionEvaluator struct {
	s Section[Datasource]
}

func (d DatasourceSectionEvaluator) Evaluate() (map[string]MasterSlaves, error) {
	mp := make(map[string]MasterSlaves)
	for name, value := range d.s.Variables {
		if err := d.getMasterSlaves(mp, name, value); err != nil {
			return nil, err
		}
	}
	return mp, nil
}

func (d DatasourceSectionEvaluator) getMasterSlaves(mp map[string]MasterSlaves, name string, value Datasource) error {
	switch val := value.Value.(type) {
	case MasterSlaves:
		mp[name] = val
		return nil
	case DatasourceTemplate:
		values, err := val.Evaluate()
		if err != nil {
			return err
		}
		for n, v := range values {
			if err1 := d.getMasterSlaves(mp, n, v); err1 != nil {
				return err1
			}
		}
	}
	return nil
}

// MasterSlaves 主从类型
type MasterSlaves struct {
	Master String `yaml:"master"`
	Slaves Enum   `yaml:"slaves,omitempty"`
}

// DatasourceTemplate 数据源模版类型
type DatasourceTemplate struct {
	global *Section[Placeholder]
	Master Template
	Slaves Template
}

func (d *DatasourceTemplate) IsZero() bool {
	return d.Master.IsZero() && d.Slaves.IsZero()
}

func (d *DatasourceTemplate) UnmarshalYAML(value *yaml.Node) error {
	type rawDatasourceTemplate struct {
		Master       string                `yaml:"master"`
		Slaves       string                `yaml:"slaves"`
		Placeholders *Section[Placeholder] `yaml:"placeholders"`
	}

	raw := &rawDatasourceTemplate{
		Placeholders: NewSection(ConfigSectionTypePlaceholders, d.global, nil, NewPlaceholder),
	}
	err := value.Decode(&raw)
	if err != nil {
		return err
	}

	log.Printf("raw.DatasourceTemplate = %#v\n", raw)
	d.Master = Template{global: d.global, Expr: raw.Master, Placeholders: *raw.Placeholders}
	d.Slaves = Template{global: d.global, Expr: raw.Slaves, Placeholders: *raw.Placeholders}
	return nil
}

func (d *DatasourceTemplate) Evaluate() (map[string]Datasource, error) {
	masters, err := d.Master.Evaluate()
	if err != nil {
		return nil, err
	}

	log.Printf("build master = %#v\n", masters)
	slaves, err := d.Slaves.Evaluate()
	if err != nil {
		return nil, err
	}

	log.Printf("build Slaves = %#v\n", slaves)
	mp := make(map[string]Datasource, len(masters))
	for name, value := range masters {
		ds := Datasource{
			Value: MasterSlaves{
				Master: String(value),
				Slaves: Enum{slaves[name]},
			},
		}
		log.Printf("build Ds %s = %#v\n", name, ds)
		mp[name] = ds
	}
	return mp, nil
}

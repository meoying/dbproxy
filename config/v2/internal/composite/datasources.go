package composite

import (
	"fmt"
	"log"

	"github.com/meoying/dbproxy/config/v2/internal/errs"
	"gopkg.in/yaml.v3"
)

type Datasources struct {
	global             *Datasources // 全局Datasources存在时,当前 Datasources 对象必为局部定义
	globalPlaceholders *Placeholders
	variables          map[string]Datasource
}

func (d *Datasources) Name() string {
	return "datasources"
}

// Find 根据变量名查找数据源 注意: 数据源中可能是模版类型
func (d *Datasources) Find(name string) (Datasource, error) {
	v, ok := d.variables[name]
	if !ok {
		return Datasource{}, fmt.Errorf("%w: %s", errs.ErrVariableNameNotFound, name)
	}
	return v, nil
}

func (d *Datasources) IsZeroValue() bool {
	return len(d.variables) == 0
}

func (d *Datasources) isGlobal() bool {
	return d.global == nil
}

func (d *Datasources) UnmarshalYAML(value *yaml.Node) error {
	variables := map[string]any{}
	err := value.Decode(&variables)
	if err != nil {
		return err
	}

	log.Printf("raw.datasources >>>  = %#v\n", variables)

	for name, val := range variables {
		if !d.isGlobal() {
			// 在局部datasources中引用
			if name == DataTypeReference {
				variables[name] = map[string]any{
					DataTypeReference: val,
				}
			}
			if name == DataTypeTemplate {
				return fmt.Errorf("%w: 不支持匿名模版", errs.ErrVariableTypeInvalid)
			}
		}
	}

	log.Printf("datasources >>> vars = %#v\n", variables)
	vars := make(map[string]Datasource, len(variables))
	builder := &DatasourceBuilder{
		global:             d.global,
		globalPlaceholders: d.globalPlaceholders,
	}
	for name, val := range variables {
		values, err1 := builder.Build(val)
		if err1 != nil {
			return fmt.Errorf("%w: %w: datasources.%s", err1, errs.ErrUnmarshalVariableFailed, name)
		}
		for n, v := range values {
			if n == "" {
				n = name
			}
			log.Printf("build new ds %s = %#v\n", n, v)
			vars[n] = v
		}
	}
	d.variables = vars
	log.Printf("解析后的 datasources = %#v\n", d)
	// 使用 Global 来判定
	// 非全局:  1) 支持,ref 匿名, 2) 匿名: template, 3) 命名 Datasource
	// ref, tmpl, Datasource
	return nil
}

func (d *Datasources) Evaluate() (map[string]MasterSlaves, error) {
	mp := make(map[string]MasterSlaves)
	for name, value := range d.variables {
		if err := d.getMasterSlaves(mp, name, value); err != nil {
			return nil, err
		}
	}
	return mp, nil
}

func (d *Datasources) getMasterSlaves(res map[string]MasterSlaves, name string, value Datasource) error {
	if value.IsMasterSlaves() {
		res[name] = value.MasterSlaves
		return nil
	} else if value.IsTemplate() {
		values, err := value.Template.Build()
		if err != nil {
			return err
		}
		for n, v := range values {
			if err := d.getMasterSlaves(res, n, v); err != nil {
				return err
			}
		}
	}
	return nil
}

type DatasourceBuilder struct {
	global             *Datasources
	globalPlaceholders *Placeholders

	Master   String                               `yaml:"master"`
	Slaves   Enum                                 `yaml:"slaves,omitempty"`
	Ref      *Reference[Datasource, *Datasources] `yaml:"ref,omitempty"`
	Template *DatasourceTemplate                  `yaml:"template,omitempty"`
}

func (d *DatasourceBuilder) isGlobalValue() bool {
	return d.global == nil
}

func (d *DatasourceBuilder) isReferenceType() bool {
	return d.Ref != nil && !d.Ref.IsZeroValue()
}

func (d *DatasourceBuilder) isTemplateType() bool {
	return d.Template != nil && !d.Template.IsZeroValue()
}

func (d *DatasourceBuilder) isMasterSlavesType() bool {
	return (d.Master != "" && d.Slaves == nil) || (d.Master != "" && d.Slaves != nil)
}

func (d *DatasourceBuilder) Build(values any) (map[string]Datasource, error) {
	d.reset()

	out, err := yaml.Marshal(values)
	if err != nil {
		return nil, err
	}

	err = yaml.Unmarshal(out, d)
	if err != nil {
		return nil, err
	}

	if d.isMasterSlavesType() {
		if d.isReferenceType() {
			return nil, fmt.Errorf("%w: master、salves不能与ref并用", errs.ErrVariableTypeInvalid)
		}
		if d.isTemplateType() {
			return nil, fmt.Errorf("%w: master、salves不能与template并用", errs.ErrVariableTypeInvalid)
		}
		return map[string]Datasource{
			"": {
				MasterSlaves: MasterSlaves{
					Master: d.Master,
					Slaves: d.Slaves,
				},
			},
		}, nil
	} else if d.isReferenceType() {
		if d.isGlobalValue() && d.Ref.IsSection(ConfigSectionDatasources) {
			return nil, fmt.Errorf("%w: 全局datasources不支持ref(引用类型)变量", errs.ErrVariableTypeInvalid)
		}
		if d.isTemplateType() {
			return nil, fmt.Errorf("%w: ref不能与template并用", errs.ErrVariableTypeInvalid)
		}
		// d.Ref.global = d.global
		return d.Ref.Build()
	} else if d.isTemplateType() {
		if d.isReferenceType() {
			return nil, fmt.Errorf("%w: template不能与ref并用", errs.ErrVariableTypeInvalid)
		}
		// d.Template.global = d.global.globalPlaceholders
		return map[string]Datasource{
			"": {
				Template: *d.Template,
			},
		}, nil
	}
	return nil, fmt.Errorf("%w", errs.ErrVariableTypeInvalid)
}

func (d *DatasourceBuilder) reset() {
	d.Master = ""
	d.Slaves = nil
	d.Ref = &Reference[Datasource, *Datasources]{global: d.global}
	d.Template = &DatasourceTemplate{global: d.globalPlaceholders}
}

// MasterSlaves 主从类型
type MasterSlaves struct {
	Master String `yaml:"master"`
	Slaves Enum   `yaml:"slaves,omitempty"`
}

func (m MasterSlaves) IsZeroValue() bool {
	return m.Master == "" && m.Slaves == nil
}

// Datasource 数据源类型
type Datasource struct {
	MasterSlaves MasterSlaves
	Template     DatasourceTemplate
}

func (d Datasource) IsZeroValue() bool {
	return d.MasterSlaves.IsZeroValue() && d.Template.IsZeroValue()
}

func (d Datasource) IsMasterSlaves() bool {
	return !d.MasterSlaves.IsZeroValue() && d.Template.IsZeroValue()
}

func (d Datasource) IsTemplate() bool {
	return d.MasterSlaves.IsZeroValue() && !d.Template.IsZeroValue()
}

// DatasourceTemplate 数据源模版类型
type DatasourceTemplate struct {
	global *Placeholders
	Master Template
	Slaves Template
}

func (d *DatasourceTemplate) IsZeroValue() bool {
	return d.Master.IsZeroValue() && d.Slaves.IsZeroValue()
}

func (d *DatasourceTemplate) UnmarshalYAML(value *yaml.Node) error {
	type rawDatasourceTemplate struct {
		Master       string       `yaml:"master"`
		Slaves       string       `yaml:"slaves"`
		Placeholders Placeholders `yaml:"placeholders"`
	}

	raw := &rawDatasourceTemplate{
		Placeholders: Placeholders{global: d.global},
	}
	err := value.Decode(&raw)
	if err != nil {
		return err
	}

	log.Printf("raw.DatasourceTemplate = %#v\n", raw)
	d.Master = Template{global: d.global, Expr: raw.Master, Placeholders: raw.Placeholders}
	d.Slaves = Template{global: d.global, Expr: raw.Slaves, Placeholders: raw.Placeholders}
	return nil
}

func (d *DatasourceTemplate) Build() (map[string]Datasource, error) {
	masters, err := d.Master.Evaluate()
	if err != nil {
		return nil, err
	}
	/*
	 build master = map[string]string{"cn_prod":"cn.master.prod.mysql.meoying.com", "cn_test":"cn.master.test.mysql.meoying.com", "hk_prod":"hk.master.prod.mysql.meoying.com", "hk_test":"hk.master.test.mysql.meoying.com"}
	*/
	log.Printf("build master = %#v\n", masters)
	slaves, err := d.Slaves.Evaluate()
	if err != nil {
		return nil, err
	}
	/*
		build Slaves = map[string]string{"cn_prod":"cn.slave.prod.mysql.meoying.com", "cn_test":"cn.slave.test.mysql.meoying.com", "hk_prod":"hk.slave.prod.mysql.meoying.com", "hk_test":"hk.slave.test.mysql.meoying.com"}
	*/
	log.Printf("build Slaves = %#v\n", slaves)
	mp := make(map[string]Datasource, len(masters))
	for name, value := range masters {
		ds := Datasource{
			MasterSlaves: MasterSlaves{
				Master: String(value),
				Slaves: Enum{slaves[name]},
			},
		}
		log.Printf("build Ds %s = %#v\n", name, ds)
		mp[name] = ds
	}
	return mp, nil
}

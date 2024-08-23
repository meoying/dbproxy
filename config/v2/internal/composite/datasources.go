package composite

import (
	"fmt"
	"log"
	"strings"

	"github.com/meoying/dbproxy/config/v2/internal/errs"
	"gopkg.in/yaml.v3"
)

type Datasources struct {
	global    *Datasources // 全局Datasources存在时,当前 Datasources 对象必为局部定义
	Variables map[string]Datasource
}

func (d *Datasources) IsZeroValue() bool {
	return len(d.Variables) == 0
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
			// 这是在非全局下的引用
			if name == DataTypeReference {
				variables[name] = map[string]any{
					DataTypeReference: val,
				}
			} else if name == DataTypeTemplate {
				variables[name] = map[string]any{
					DataTypeTemplate: val,
				}
			}
		}
	}

	log.Printf("datasources >>> vars = %#v\n", variables)
	vars := make(map[string]Datasource, len(variables))
	builder := &DatasourceBuilder{
		global: d.global,
	}

	for name, val := range variables {
		values, err1 := builder.Build(val)
		if err1 != nil {
			return err1
		}
		// if name == DataTypeReference || name == DataTypeTemplate {
		// 	for n, v := range values {
		// 		vars[n] = v
		// 	}
		// } else {
		// 	vars[name] = values[""]
		// }
		for n, v := range values {
			if n == "" {
				n = name
			}
			log.Printf("build new ds %s = %#v\n", n, v)
			vars[n] = v
		}

	}
	d.Variables = vars

	// 使用 Global 来判定
	// 非全局:  1) 支持,ref 匿名, 2) 匿名: template, 3) 命名 Datasource
	// ref, tmpl, Datasource

	return nil
}

type DatasourceBuilder struct {
	global   *Datasources
	Master   String              `yaml:"master"`
	Slaves   Enum                `yaml:"slaves,omitempty"`
	Ref      []string            `yaml:"ref,omitempty"`
	Template *DatasourceTemplate `yaml:"template,omitempty"`
}

func (d *DatasourceBuilder) isGlobal() bool {
	return d.global == nil
}

func (d *DatasourceBuilder) Build(values any) (map[string]Datasource, error) {
	out, err := yaml.Marshal(values)
	if err != nil {
		return nil, err
	}
	err = yaml.Unmarshal(out, d)
	if err != nil {
		return nil, err
	}
	defer d.clear()
	if d.Master != "" {
		// TODO: 不能有ref
		//       不能有template
		// 直接构造
		return map[string]Datasource{
			"": {
				Master: d.Master,
				Slaves: d.Slaves,
			},
		}, nil
	} else if len(d.Ref) > 0 {
		// 引用类型
		if d.isGlobal() {
			return nil, fmt.Errorf("%w: 全局datasources不支持ref(引用类型)变量", errs.ErrVariableTypeInvalid)
		}
		// TODO: 不能有slaves
		// TODO: 不能有template

		paths := d.Ref
		mp := make(map[string]Datasource, len(paths))
		for _, path := range paths {
			varInfo := strings.SplitN(path, ".", 2)
			varType, varName := varInfo[0], varInfo[1]
			log.Printf("引用路径信息 = %#v\n", varInfo)
			t, ok := d.global.Variables[varName]
			log.Printf("global = %#v\n", d.global.Variables)
			if varType != ConfigSectionDatasources || !ok {
				return nil, fmt.Errorf("%w: %s", errs.ErrReferencePathInvalid, path)
			}
			mp[varName] = t
		}
		return mp, nil
	} else if d.Template != nil {
		// todo: 不能有slaves
		return d.Template.Build()
	}
	return nil, nil
}

func (d *DatasourceBuilder) clear() {
	d.Master = ""
	d.Slaves = nil
	d.Ref = nil
	d.Template = nil
}

// Datasource 数据源类型
type Datasource struct {
	Master String `yaml:"master"`
	Slaves Enum   `yaml:"slaves,omitempty"`
}

type DatasourceTemplate struct {
	// global       *Placeholders
	Master       Template
	Slaves       Template
	Placeholders Placeholders
}

func (d *DatasourceTemplate) UnmarshalYAML(value *yaml.Node) error {
	type rawDatasourceTemplate struct {
		Master       string       `yaml:"master"`
		Slaves       string       `yaml:"slaves"`
		Placeholders Placeholders `yaml:"placeholders"`
	}

	var raw rawDatasourceTemplate
	err := value.Decode(&raw)
	if err != nil {
		return err
	}

	log.Printf("raw.DatasourceTemplate = %#v\n", raw)
	d.Master = Template{Expr: raw.Master, Placeholders: raw.Placeholders}
	d.Slaves = Template{Expr: raw.Slaves, Placeholders: raw.Placeholders}
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
			Master: String(value),
			Slaves: Enum{slaves[name]},
		}
		log.Printf("build Ds %s = %#v\n", name, ds)
		mp[name] = ds
	}
	return mp, nil
}

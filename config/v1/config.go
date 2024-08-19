package v1

import (
	"errors"
	"fmt"
	"log"
	"reflect"
	"regexp"
	"strings"

	"gopkg.in/yaml.v3"
)

const (
	ConfigFieldVariables   = "variables"
	ConfigFieldDatabases   = "databases"
	ConfigFieldDatasources = "datasources"
	ConfigFieldTables      = "tables"

	DataTypeString    = "string"
	DataTypeEnum      = "enum"
	DataTypeHash      = "hash"
	DataTypeTemplate  = "template"
	DataTypeReference = "ref"

	DataTypeVariable   = "variable"
	DataTypeDatabase   = "database"
	DataTypeDatasource = "datasource"
	DataTypeTable      = "_table_" // 不能与关键字相同
	DataTypeSharding   = "sharding"
)

var (
	ErrVariableNameNotFound          = errors.New("变量名称找不到")
	ErrVariableTypeInvalid           = errors.New("变量类型非法")
	ErrUnmarshalVariableFieldFailed  = errors.New("反序列化类型属性失败")
	ErrUnmarshalVariableFailed       = errors.New("反序列化变量失败")
	ErrVariableTypeNotEvaluable      = errors.New("变量类型不可求值")
	ErrReferencedVariableTypeInvalid = errors.New("引用的变量类型非法")
	ErrReferencePathInvalid          = errors.New("引用路径非法")
)

type (
	stringEvaluator interface {
		Evaluate() ([]string, error)
	}
)

// Config 配置结构体
type Config struct {
	Variables   map[string]any `yaml:"variables,omitempty"`
	Databases   map[string]any `yaml:"databases,omitempty"`
	Datasources map[string]any `yaml:"datasources,omitempty"`
	Tables      map[string]any `yaml:"tables"`
}

func (c *Config) UnmarshalYAML(value *yaml.Node) error {
	type rawConfig struct {
		Variables   map[string]any `yaml:"variables"`
		Databases   map[string]any `yaml:"databases"`
		Datasources map[string]any `yaml:"datasources"`
		Tables      map[string]any `yaml:"tables"`
	}
	var raw rawConfig
	if err := value.Decode(&raw); err != nil {
		return err
	}

	c.Variables = raw.Variables
	c.Databases = raw.Databases
	c.Datasources = raw.Datasources
	c.Tables = raw.Tables

	for typ, section := range map[string]map[string]any{
		DataTypeVariable:   c.Variables,
		DataTypeDatabase:   c.Databases,
		DataTypeDatasource: c.Datasources,
		DataTypeTable:      c.Tables,
	} {
		err := unmarshal(c, typ, section)
		if err != nil {
			return err
		}
	}
	log.Printf("config.Datasources: %#v\n", c.Datasources)
	return nil
}

func unmarshal(c *Config, typ string, variables map[string]any) error {
	log.Printf("unmarshal typ = %s, variables = %#v\n", typ, variables)
	for name, value := range variables {
		variable, err := unmarshalUntypedVariable(c, typ, name, value)
		if err != nil {
			return err
		}
		variables[name] = variable
	}

	return nil
}

// unmarshalUntypedVariable 反序列化未类型化的变量
func unmarshalUntypedVariable(c *Config, dataType, name string, value any) (any, error) {
	log.Printf("unmarshalUntypedVariable type = %s, name = %s, value = %#v\n", dataType, name, value)
	var untypedVal map[string]any
	switch val := value.(type) {
	case String, Enum, Hash, Template, Ref, Variable, Database, Datasource, Table:
		return value, nil
	case map[string]any:
		if dataType != "" {
			untypedVal = map[string]any{
				dataType: val,
			}
		} else {
			untypedVal = val
		}
	case []any:
		vv, elemType, err := convertArrayValues(val)
		if err != nil {
			return nil, err
		}
		if dataType != "" {
			untypedVal = map[string]any{
				dataType: map[string]any{
					elemType: vv,
				},
			}
		} else {
			return vv, nil
		}
	case string:
		if dataType != "" {
			untypedVal = map[string]any{
				dataType: map[string]any{
					DataTypeString: val,
				},
			}
		} else {
			return String(val), nil
		}
	}
	log.Printf("unmarshalUntypedVariable(%s) untyped = %#v\n", name, untypedVal)
	typedVal, err := unmarshalDataType(c, name, untypedVal)
	if err != nil {
		return nil, err
	}
	log.Printf("unmarshalUntypedVariable(%s) typed = %#v\n", name, typedVal)
	return typedVal, nil
}

func convertArrayValues(val []any) (any, string, error) {
	switch val[0].(type) {
	case string:
		strs := make(Enum, len(val))
		for i := range val {
			strs[i] = val[i].(string)
		}
		return strs, DataTypeEnum, nil
	default:
		return nil, "unknown", fmt.Errorf("未知的数组元素类型: %t", val[0])
	}
}

func unmarshalDataType(c *Config, name string, rawVal map[string]any) (any, error) {
	dataTypes := map[string]yaml.Unmarshaler{
		DataTypeTemplate: &Template{
			varName: name,
			config:  c,
		},
		DataTypeReference: &Ref{
			varName: name,
			config:  c,
		},
		DataTypeHash: &Hash{varName: name},

		DataTypeVariable: &Variable{
			varName: name,
			config:  c,
		},
		DataTypeDatabase: &Database{
			varName: name,
			config:  c,
		},
		DataTypeDatasource: &Datasource{
			varName: name,
			config:  c,
		},
		DataTypeTable: &Table{
			varName: name,
			config:  c,
		},
		DataTypeSharding: &Sharding{
			varName: name,
			config:  c,
		},
	}
	for key, typ := range dataTypes {
		if r, ok := rawVal[key]; ok {
			err := unmarshalDataTypeValue(r, typ)
			if err != nil {
				return nil, fmt.Errorf("%w: %q: %s", ErrVariableTypeInvalid, name, err)
			}
			return reflect.ValueOf(typ).Elem().Interface(), nil
		}
	}
	return nil, fmt.Errorf("%w: %q", ErrVariableTypeInvalid, name)
}

func unmarshalDataTypeValue(rawVal any, typ yaml.Unmarshaler) error {
	log.Printf("rawVal = %#v\n", rawVal)
	node := &yaml.Node{}
	if err := node.Encode(rawVal); err != nil {
		return err
	}
	if err := typ.UnmarshalYAML(node); err != nil {
		return err
	}
	return nil
}

func (c *Config) VariableNames() []string {
	var keys []string
	for k := range c.Variables {
		keys = append(keys, k)
	}
	return keys
}

func (c *Config) VariableByName(name string) (any, error) {
	if v, ok := c.Variables[name]; ok {
		return v, nil
	}
	return nil, fmt.Errorf("%w: %q", ErrVariableNameNotFound, name)
}

func (c *Config) DatasourceNames() []string {
	var keys []string
	for k := range c.Datasources {
		keys = append(keys, k)
	}
	return keys
}

func (c *Config) DatasourceByName(name string) (any, error) {
	if v, ok := c.Datasources[name]; ok {
		return v, nil
	}
	return nil, fmt.Errorf("%w: %q", ErrVariableNameNotFound, name)
}

func (c *Config) DatabaseNames() []string {
	var keys []string
	for k := range c.Databases {
		keys = append(keys, k)
	}
	return keys
}

func (c *Config) DatabaseByName(name string) (any, error) {
	if v, ok := c.Databases[name]; ok {
		return v, nil
	}
	return nil, fmt.Errorf("%w: %q", ErrVariableNameNotFound, name)
}

func (c *Config) TableNames() []string {
	var keys []string
	for k := range c.Tables {
		keys = append(keys, k)
	}
	return keys
}

func (c *Config) TableByName(name string) (any, error) {
	if v, ok := c.Tables[name]; ok {
		return v, nil
	}
	return nil, fmt.Errorf("%w: %q", ErrVariableNameNotFound, name)
}

type String string

func (s String) Evaluate() ([]string, error) {
	return []string{string(s)}, nil
}

// Enum 枚举类型
type Enum []string

func (e Enum) Evaluate() ([]string, error) {
	return e, nil
}

// Hash 类型
type Hash struct {
	varName string
	Key     string `yaml:"key"`
	Base    int    `yaml:"base"`
}

func (h *Hash) isZeroValue() bool {
	return h.Key == "" && h.Base == 0
}

func (h *Hash) UnmarshalYAML(value *yaml.Node) error {
	type rawHash struct {
		Key  string `yaml:"key"`
		Base int    `yaml:"base"`
	}
	var raw rawHash
	if err := value.Decode(&raw); err != nil {
		return err
	}
	h.Key = strings.TrimSpace(raw.Key)
	if h.Key == "" {
		return fmt.Errorf("%w: hash.key = %q", ErrUnmarshalVariableFieldFailed, h.Key)
	}
	h.Base = raw.Base
	if h.Base <= 0 {
		return fmt.Errorf("%w: hash.base = %d", ErrUnmarshalVariableFieldFailed, h.Base)
	}
	return nil
}

func (h *Hash) Evaluate() ([]string, error) {
	strs := make([]string, h.Base)
	for i := 0; i < h.Base; i++ {
		strs[i] = fmt.Sprintf("%d", i)
	}
	return strs, nil
}

// Template 模版类型
type Template struct {
	varName      string
	Expr         string         `yaml:"expr"`
	Placeholders map[string]any `yaml:"placeholders"`
	config       *Config
}

func (t *Template) isZeroValue() bool {
	return t.Expr == "" && len(t.Placeholders) == 0
}

func (t *Template) UnmarshalYAML(value *yaml.Node) error {
	type rawTemplate struct {
		Expr         string         `yaml:"expr"`
		Placeholders map[string]any `yaml:"placeholders"`
	}
	var raw rawTemplate
	if err := value.Decode(&raw); err != nil {
		return err
	}
	t.Expr = strings.TrimSpace(raw.Expr)
	if len(t.Expr) == 0 {
		return fmt.Errorf("%w: template.expr = %q", ErrUnmarshalVariableFieldFailed, t.Expr)
	}
	t.Placeholders = raw.Placeholders
	if len(t.Placeholders) == 0 {
		return fmt.Errorf("%w: template.placeholders = %q", ErrUnmarshalVariableFieldFailed, t.Placeholders)
	}
	for _, ph := range t.extractPlaceholders() {
		if _, ok := t.Placeholders[ph]; !ok {
			return fmt.Errorf("%w: template.placeholders 缺少占位符 %s", ErrUnmarshalVariableFieldFailed, ph)
		}
	}
	for ph := range t.Placeholders {
		if !strings.Contains(t.Expr, fmt.Sprintf("${%s}", ph)) {
			return fmt.Errorf("%w: template.expr 缺少占位符 %s", ErrUnmarshalVariableFieldFailed, ph)
		}
	}

	err := unmarshal(t.config, "", t.Placeholders)
	if err != nil {
		return err
	}

	for varName := range t.Placeholders {
		ref, ok := t.Placeholders[varName].(Ref)
		if !ok {
			continue
		}
		t.Placeholders[varName] = ref.Value
	}
	return nil
}

func (t *Template) extractPlaceholders() []string {
	re := regexp.MustCompile(`\$\{([^}]+)\}`)
	matches := re.FindAllStringSubmatch(t.Expr, -1)
	var results []string
	for _, match := range matches {
		if len(match) > 1 {
			results = append(results, match[1]) // append the actual key found
		}
	}
	return results
}

func (t *Template) Evaluate() ([]string, error) {

	var results []string

	// var evaluateFunc func(expr string, placeholders []string) error
	//
	// evaluateFunc = func(expr string, placeholders []string) error {
	// 	if len(placeholders) == 0 {
	// 		results = append(results, expr)
	// 		return nil
	// 	}
	//
	// 	i := 0
	// 	ph := placeholders[i]
	// 	expr = strings.Replace(expr, "${"+ph+"}", "%s", 1)
	//
	// 	evaluator, err := t.getStringEvaluator(ph)
	// 	if err != nil {
	// 		return err
	// 	}
	//
	// 	values, err := evaluator.Evaluate()
	// 	if err != nil {
	// 		return err
	// 	}
	//
	// 	for _, v := range values {
	// 		err = evaluateFunc(fmt.Sprintf(expr, v), placeholders[i+1:])
	// 		if err != nil {
	// 			return err
	// 		}
	// 	}
	// 	return nil
	// }

	if err := t.evaluate(&results, t.Expr, t.extractPlaceholders()); err != nil {
		return nil, err
	}

	return results, nil
}

func (t *Template) evaluate(results *[]string, expr string, placeholders []string) error {
	if len(placeholders) == 0 {
		*results = append(*results, expr)
		return nil
	}

	i := 0
	ph := placeholders[i]
	expr = strings.Replace(expr, "${"+ph+"}", "%s", 1)

	evaluator, err := t.getStringEvaluator(ph)
	if err != nil {
		return err
	}

	values, err := evaluator.Evaluate()
	if err != nil {
		return err
	}

	for _, v := range values {
		err = t.evaluate(results, fmt.Sprintf(expr, v), placeholders[i+1:])
		if err != nil {
			return err
		}
	}
	return nil
}

func (t *Template) getStringEvaluator(ph string) (stringEvaluator, error) {
	value := t.Placeholders[ph]
	switch v := value.(type) {
	case String:
		return v, nil
	case Enum:
		return v, nil
	case Hash:
		return &v, nil
	default:
		return nil, fmt.Errorf("%w: %q", ErrVariableTypeNotEvaluable, ph)
	}
}

type Ref struct {
	varName string
	varType string
	path    string
	Value   any
	config  *Config
}

func (r *Ref) isZeroValue() bool {
	var zero any
	return r.Value == zero
}

func (r *Ref) UnmarshalYAML(value *yaml.Node) error {
	var path string
	if err := value.Decode(&path); err != nil {
		return err
	}
	if path == "" {
		return fmt.Errorf("%w: %q", ErrUnmarshalVariableFailed, r.varName)
	}
	log.Printf("引用地址 path = %#v\n", path)
	v, t, err := unmarshalReferencedVariable(r.config, strings.Split(path, "."))
	if err != nil {
		return err
	}
	log.Printf("获取到的引用类型 v = %#v, t = %s", v, t)

	// TODO: 再这里解包,还是到具体的类型中?
	for range strings.Split(path, ".") {
		switch t {
		case DataTypeVariable:
			vv := v.(Variable)
			t = vv.varType
			v = vv.Value
		case DataTypeDatabase:
			vv := v.(Database)
			t = vv.varType
			v = vv.Value
		case DataTypeHash:
			vv := v.(Hash)
			vv.varName = r.varName
			v = vv
		case DataTypeTemplate:
			vv := v.(Template)
			vv.varName = r.varName
			v = vv
		default:
			continue
		}
	}
	log.Printf("准备填入ref v = %#v, t = %s", v, t)
	// var refVal any
	// switch t {
	// case DataTypeHash:
	// 	vv := v.(Hash)
	// 	vv.varName = r.varName
	// 	refVal = vv
	// case DataTypeTemplate:
	// 	vv := v.(Template)
	// 	vv.varName = r.varName
	// }
	// r.Value = refVal
	r.varType = t
	r.path = path
	r.Value = v
	log.Printf("最终的引用类型 ref = %#v\n", r)
	return nil
}

func unmarshalReferencedVariable(config *Config, paths []string) (any, string, error) {
	var values map[string]any
	var varType string

	switch paths[0] {
	case ConfigFieldVariables:
		log.Printf("config.Variables = %#v\n", config.Variables)
		values = config.Variables
		varType = DataTypeVariable
	case ConfigFieldDatabases:
		log.Printf("config.Databases = %#v\n", config.Databases)
		values = config.Databases
		varType = DataTypeDatabase
	case ConfigFieldDatasources:
		log.Printf("config.Datasources = %#v\n", config.Datasources)
		values = config.Datasources
		varType = DataTypeDatasource
	default:
		return nil, fmt.Sprintf("unknow pathType - %s", paths[0]), fmt.Errorf("%w: %s", ErrVariableNameNotFound, paths[0])
	}

	if len(paths) != 2 {
		return nil, "", fmt.Errorf("%w: %s", ErrReferencePathInvalid, strings.Join(paths, "."))
	}

	varName := paths[1]
	// 变量名存在
	v, ok := values[varName]
	if !ok {
		return nil, varType, fmt.Errorf("%w: %s", ErrReferencePathInvalid, strings.Join(paths, "."))
	}

	value, err := unmarshalUntypedVariable(config, varType, varName, v)
	if err != nil {
		return nil, varType, err
	}
	values[varName] = value

	return value, varType, nil
}

// Variable 变量类型
type Variable struct {
	varName string
	varType string
	Value   any
	config  *Config
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
		Tmpl: &Template{varName: v.varName, config: v.config},
		Ref:  &Ref{varName: v.varName, config: v.config},
		Hash: &Hash{varName: v.varName},
	}
	if err := value.Decode(raw); err != nil {
		return err
	}

	log.Printf("raw.Variables = %#v\n", raw)

	if raw.Str == "" && len(raw.Enum) == 0 &&
		raw.Hash.isZeroValue() && raw.Tmpl.isZeroValue() && raw.Ref.isZeroValue() {
		return fmt.Errorf("%w: variables.%q", ErrUnmarshalVariableFailed, v.varName)
	}

	if raw.Str != "" {
		v.varType = DataTypeString
		v.Value = String(raw.Str)
	} else if len(raw.Enum) > 0 {
		v.varType = DataTypeEnum
		v.Value = Enum(raw.Enum)
	} else if !raw.Hash.isZeroValue() {
		hash := *raw.Hash
		hash.varName = v.varName
		v.varType = DataTypeHash
		v.Value = hash
	} else if !raw.Tmpl.isZeroValue() {
		tmpl := *raw.Tmpl
		tmpl.varName = v.varName
		v.varType = DataTypeTemplate
		v.Value = tmpl
	} else if !raw.Ref.isZeroValue() {
		log.Printf("Variable 中 解析到的 ref = %#v\n", raw.Ref)
		v.varType = raw.Ref.varType
		v.Value = raw.Ref.Value
	}
	return nil
}

// Database 数据库类型
type Database struct {
	varName string
	varType string
	Value   any
	config  *Config
}

func (d *Database) UnmarshalYAML(value *yaml.Node) error {
	type rawDatabase struct {
		Tmpl *Template `yaml:"template,omitempty"`
		Str  string    `yaml:"string,omitempty"`
		Ref  *Ref      `yaml:"ref,omitempty"`
	}
	raw := &rawDatabase{
		Tmpl: &Template{varName: d.varName, config: d.config},
		Ref:  &Ref{varName: d.varName, config: d.config},
	}
	if err := value.Decode(raw); err != nil {
		return err
	}
	log.Printf("raw.Database = %#v\n", raw)

	if raw.Str == "" && raw.Tmpl.isZeroValue() && raw.Ref.isZeroValue() {
		return fmt.Errorf("%w: %s.%q", ErrUnmarshalVariableFailed, ConfigFieldDatabases, d.varName)
	}

	if raw.Str != "" {
		d.varType = DataTypeString
		d.Value = String(raw.Str)
	}

	if !raw.Tmpl.isZeroValue() {
		tmpl := *raw.Tmpl
		tmpl.varName = d.varName
		tmpl.config = d.config
		d.varType = DataTypeTemplate
		d.Value = tmpl
	}

	if !raw.Ref.isZeroValue() {
		log.Printf("Database 中 解析到的 ref = %#v\n", raw.Ref)
		d.varType = raw.Ref.varType
		d.Value = raw.Ref.Value
	}

	log.Printf("Databases ===...var = %s, %#v\n", d.varName, d.config.Databases)

	return nil
}

// Datasource 数据源类型
type Datasource struct {
	varName string
	Master  String `yaml:"master"`
	Slave   any    `yaml:"slave"`
	config  *Config
}

func (d *Datasource) UnmarshalYAML(value *yaml.Node) error {
	type rawDatasource struct {
		Master string `yaml:"master"`
		Slave  any    `yaml:"slave,omitempty"`
		Ref    *Ref   `yaml:"ref,omitempty"`
	}
	raw := &rawDatasource{
		Ref: &Ref{varName: d.varName, config: d.config},
	}
	if err := value.Decode(raw); err != nil {
		return err
	}

	log.Printf("raw.Datasource = %#v\n", raw)
	var zero any
	if raw.Master == "" && raw.Ref.isZeroValue() {
		return fmt.Errorf("%w: master = %q", ErrUnmarshalVariableFieldFailed, raw.Master)
	} else if raw.Master == "" && raw.Slave == zero && raw.Ref.isZeroValue() {
		return fmt.Errorf("%w: slave = %+v", ErrUnmarshalVariableFieldFailed, raw.Slave)
	} else if raw.Master != "" && !raw.Ref.isZeroValue() || raw.Slave != zero && !raw.Ref.isZeroValue() {
		return fmt.Errorf("ref属性不可与Master/Slave属性组合使用")
	}

	d.Master = String(raw.Master)

	if raw.Slave != zero {
		switch slave := raw.Slave.(type) {
		case string:
			d.Slave = String(slave)
		case map[string]any:
			log.Printf("准备反序列化slave raw.Slave = %#v", raw.Slave)
			dataType, err := unmarshalDataType(d.config, "slave", slave)
			if err != nil {
				return err
			}
			log.Printf("获取到的%s, slave = %#v\n", d.varName, dataType)
			d.Slave = dataType
		default:
			return fmt.Errorf("未支持的slave类型 %t\n", slave)
		}
	}

	if !raw.Ref.isZeroValue() {
		log.Printf("数据源获取到的 ref = %#v\n", raw.Ref)
		ds, ok := raw.Ref.Value.(Datasource)
		if !ok {
			return fmt.Errorf("%w: %s", ErrReferencedVariableTypeInvalid, d.varName)
		}
		d.Master = ds.Master
		d.Slave = ds.Slave
	}
	return nil
}

type Sharding struct {
	varName    string
	config     *Config
	Datasource Datasource `yaml:"datasource"`
	Database   Database   `yaml:"database"`
	Table      Variable   `yaml:"table"`
}

func (s *Sharding) UnmarshalYAML(value *yaml.Node) error {
	type rawSharding struct {
		Datasource map[string]any `yaml:"datasource"`
		Database   map[string]any `yaml:"database"`
		Table      any            `yaml:"table"`
	}
	log.Printf("sharding raw之前的原始值 = %#v\n", s)
	// raw := &rawSharding{
	// 	Datasource: &Datasource{varName: s.varName, config: s.config},
	// 	Database:   &Database{varName: s.varName, config: s.config},
	// 	Table:      &Variable{varName: s.varName, config: s.config},
	// }
	var raw rawSharding
	if err := value.Decode(&raw); err != nil {
		return err
	}

	log.Printf("解析 raw.Sharding = %#v\n", raw)
	log.Printf("before ds = %#v\n", raw.Datasource)
	log.Printf("before db = %#v\n", raw.Database)
	log.Printf("before tb = %#v\n", raw.Table)

	if len(raw.Datasource) == 0 {
		return fmt.Errorf("%w: %s.sharding.datasource", ErrUnmarshalVariableFieldFailed, s.varName)
	}
	v, err := unmarshalUntypedVariable(s.config, DataTypeDatasource, s.varName, raw.Datasource)
	if err != nil {
		return err
	}
	ds := v.(Datasource)
	s.Datasource = ds
	log.Printf("解析ds成功! = %#v\n", s.Datasource)

	if len(raw.Database) != 1 {
		return fmt.Errorf("%w: %s.sharding.database", ErrUnmarshalVariableFieldFailed, s.varName)
	}

	// 将匿名引用调整到可以解析的程度,使用引用路径作为变量名
	mp := make(map[string]any)
	for k, v := range raw.Database {
		if k == "ref" {
			mp[v.(string)] = map[string]any{k: v}
			continue
		}
		mp[k] = v
	}
	err = unmarshal(s.config, DataTypeDatabase, mp)
	if err != nil {
		return err
	}
	var varName string
	for key := range mp {
		varName = key
	}
	db := mp[varName].(Database)
	db.varName = varName
	s.Database = db
	log.Printf("解析db成功! = %#v\n", s.Database)

	v, err = unmarshalUntypedVariable(s.config, DataTypeVariable, s.varName, raw.Table)
	if err != nil {
		return err
	}
	tb := v.(Variable)
	s.Table = tb
	log.Printf("解析tb成功! = %#v\n", s.Table)
	return nil
}

type Table struct {
	varName  string
	varType  string
	config   *Config
	Sharding Sharding `yaml:"sharding"`
}

func (t *Table) UnmarshalYAML(value *yaml.Node) error {

	type rawTable struct {
		Sharding *Sharding `yaml:"sharding"`
	}
	raw := &rawTable{
		Sharding: &Sharding{varName: t.varName, config: t.config},
	}
	if err := value.Decode(&raw); err != nil {
		return err
	}

	t.Sharding = *raw.Sharding
	t.varType = DataTypeSharding
	return nil
}

func (t *Table) AlgorithmInfo() map[string]map[string]any {
	mp := make(map[string]map[string]any)
	t.setDatabaseAlgorithmInfo(mp)
	t.setDatasourceAlgorithmInfo(mp)
	t.setTableAlgorithmInfo(mp)
	return mp
}

func (t *Table) setDatabaseAlgorithmInfo(mp map[string]map[string]any) {
	mp["database"] = map[string]any{
		"key":         "",
		"expr":        "",
		"base":        32,
		"notSharding": true,
	}
}

func (t *Table) setDatasourceAlgorithmInfo(mp map[string]map[string]any) {
	mp["datasource"] = map[string]any{
		"key":         "",
		"expr":        "",
		"base":        32,
		"notSharding": true,
	}
}

func (t *Table) setTableAlgorithmInfo(mp map[string]map[string]any) {
	// value := t.Sharding.Table.Value

	mp["table"] = map[string]any{
		"key":         "",
		"expr":        "",
		"base":        32,
		"notSharding": true,
	}
}

func (t *Table) DSNInfo() map[string][]string {

	mp := make(map[string][]string)

	t.setDSNInfoMaster2Slave(mp)
	t.setDSNInfoDBName2Master(mp)
	t.setDSNInfoDs2DBs(mp)

	return mp
}

func (t *Table) setDSNInfoDs2DBs(mp map[string][]string) {
	mp["ds2db"] = []string{}
}

func (t *Table) setDSNInfoMaster2Slave(mp map[string][]string) {
	mp["master2Salve"] = []string{}
}

func (t *Table) setDSNInfoDBName2Master(mp map[string][]string) {
	mp["db2master"] = []string{}
}

func (t *Table) MasterDSN() string {
	return string(t.Sharding.Datasource.Master)
}

func (t *Table) SlaveDSN() ([]string, error) {
	r, err := t.evaluate(t.Sharding.Datasource.Slave)
	if err != nil {
		return nil, fmt.Errorf("获取从库DSN集合失败: %w", err)
	}
	return r, nil
}

func (t *Table) evaluate(value any) ([]string, error) {
	switch val := value.(type) {
	case String:
		return []string{string(val)}, nil
	case Enum:
		return val, nil
	case Template:
		return val.Evaluate()
	default:
		return []string{}, fmt.Errorf("未支持的类型 %t", val)
	}
}

func (t *Table) DatabaseNames() ([]string, error) {
	r, err := t.evaluate(t.Sharding.Database.Value)
	if err != nil {
		return nil, fmt.Errorf("获取数据库名称集合失败: %w", err)
	}
	return r, nil
}

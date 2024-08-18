package v1

import (
	"errors"
	"fmt"
	"log"
	"regexp"
	"strings"

	"gopkg.in/yaml.v3"
)

const (
	ConfigFieldVariables   = "variables"
	ConfigFieldDatabases   = "databases"
	ConfigFieldDatasources = "datasources"
	ConfigFieldTables      = "tables"

	DataTypeTemplate   = "template"
	DataTypeReference  = "ref"
	DataTypeHash       = "hash"
	DataTypeDatasource = "datasource"
)

var (
	ErrVariableNameNotFound         = errors.New("变量名称找不到")
	ErrVariableTypeInvalid          = errors.New("变量类型非法")
	ErrUnmarshalVariableFieldFailed = errors.New("反序列化类型属性失败")
	ErrVariableTypeNotEvaluable     = errors.New("变量类型不可求值")
)

// Config 配置结构体
type Config struct {
	Variables   map[string]any `yaml:"variables"`
	Databases   map[string]any `yaml:"databases"`
	Datasources map[string]any `yaml:"datasources"`
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
	err := unmarshal(c, c.Variables)
	if err != nil {
		return err
	}

	c.Databases = raw.Databases
	err = unmarshal(c, c.Databases)
	if err != nil {
		return err
	}

	c.Datasources = raw.Datasources
	err = unmarshal(c, c.Datasources)
	if err != nil {
		return err
	}

	c.Tables = raw.Tables
	err = unmarshal(c, c.Tables)
	if err != nil {
		return err
	}
	log.Printf("config.Datasources: %#v\n", c.Datasources)
	return nil
}

func unmarshal(c *Config, values map[string]any) error {
	for k, v := range values {
		switch val := v.(type) {
		case map[string]any:
			vv, err := unmarshalDataType(c, k, val)
			if err != nil {
				return err
			}
			values[k] = vv
		case []any:
			// Check if the elements are []string or []map[string]any
			if len(val) == 0 {
				values[k] = val
				return nil
			}
			switch val[0].(type) {
			case string:
				strs := make(Enum, len(val))
				for i := range val {
					strs[i] = val[i].(string)
				}
				values[k] = strs
			case map[string]any:
				typedVals := make([]any, len(val))
				log.Printf("handle val = %#v, k = %s\n", val, k)
				for i := range val {
					mapVal := val[i].(map[string]any)
					log.Printf("handle val[i] = %#v, k = %s\n", mapVal, k)
					typedVal, err := unmarshalDataType(c, k, mapVal)
					if err != nil {
						return err
					}
					log.Printf("handle typedVal = %#v\n", typedVal)
					typedVals[i] = typedVal
				}
				var vv any
				vv = typedVals
				values[k] = vv
				log.Printf("post := %#v\n", values[k])
			default:
				// Handle unexpected types
				return errors.New("unexpected type in array")
			}
		case string:
			values[k] = String(val)
		}
	}
	return nil
}

func unmarshalDataType(c *Config, name string, rawVal map[string]any) (any, error) {
	dataTypes := map[string]yaml.Unmarshaler{
		DataTypeTemplate: &Template{
			config: c,
		},
		DataTypeReference: &Reference{
			values: make(map[string]any),
			config: c,
		},
		DataTypeHash:       &Hash{},
		DataTypeDatasource: &Datasource{config: c},
	}
	for key, typ := range dataTypes {
		if r, ok := rawVal[key]; ok {
			err := unmarshalDataTypeValue(r, typ)
			if err != nil {
				return nil, fmt.Errorf("%w: %q: %s", ErrVariableTypeInvalid, name, err)
			}
			return typ, nil
		}
	}
	return nil, fmt.Errorf("%w: %q", ErrVariableTypeInvalid, name)
}

func unmarshalDataTypeValue(rawVal any, typ yaml.Unmarshaler) error {
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

func (c *Config) GetVariableByName(name string) (any, error) {
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

func (c *Config) GetDatasourceByName(name string) (any, error) {
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

func (c *Config) GetDatabaseByName(name string) (any, error) {
	if v, ok := c.Databases[name]; ok {
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

// Reference 引用类型
type Reference struct {
	values map[string]any
	config *Config
}

func (r *Reference) UnmarshalYAML(value *yaml.Node) error {
	var paths []string
	if err := value.Decode(&paths); err != nil {
		return err
	}
	for _, path := range paths {
		val, err := find(r.config, strings.Split(path, "."))
		if err != nil {
			return err
		}
		r.values[path] = val
	}
	return unmarshal(r.config, r.values)
}

func find(config *Config, paths []string) (any, error) {
	var values map[string]any

	switch paths[0] {
	case ConfigFieldVariables:
		log.Printf("config.Variables = %#v\n", config.Variables)
		values = config.Variables
	case ConfigFieldDatabases:
		log.Printf("config.Databases = %#v\n", config.Databases)
		values = config.Databases
	case ConfigFieldDatasources:
		log.Printf("config.Datasources = %#v\n", config.Datasources)
		values = config.Datasources
	default:
		return nil, fmt.Errorf("%w: %s", ErrVariableNameNotFound, paths[0])
	}
	var val any
	for i, path := range paths[1:] {
		val = values[path]
		if len(paths[1:])-1 != i {
			values = val.(map[string]any)
		}
	}
	return val, nil
}

type stringEvaluator interface {
	Evaluate() ([]string, error)
}

func (r *Reference) Evaluate() ([]string, error) {
	var results []string
	for k, v := range r.values {
		evaluator, ok := v.(stringEvaluator)
		if !ok {
			return nil, fmt.Errorf("%w: %q", ErrVariableTypeNotEvaluable, k)
		}
		values, err := evaluator.Evaluate()
		if err != nil {
			return nil, err
		}
		results = append(results, values...)
	}
	return results, nil
}

// Hash 类型
type Hash struct {
	Key  string `yaml:"key"`
	Base int    `yaml:"base"`
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
	Expr         string                 `yaml:"expr"`
	Placeholders map[string]interface{} `yaml:"placeholders"`
	config       *Config
}

func (t *Template) UnmarshalYAML(value *yaml.Node) error {
	type rawTemplate struct {
		Expr         string                 `yaml:"expr"`
		Placeholders map[string]interface{} `yaml:"placeholders"`
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

	return unmarshal(t.config, t.Placeholders)
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
	var evaluateFunc func(expr string, placeholders []string) error

	evaluateFunc = func(expr string, placeholders []string) error {
		if len(placeholders) == 0 {
			results = append(results, expr)
			return nil
		}

		i := 0
		ph := placeholders[i]
		expr = strings.Replace(expr, "${"+ph+"}", "%s", 1)

		evaluator, ok := t.Placeholders[ph].(stringEvaluator)
		if !ok {
			return fmt.Errorf("%w: %q", ErrVariableTypeNotEvaluable, ph)
		}

		values, err := evaluator.Evaluate()
		if err != nil {
			return err
		}

		for _, v := range values {
			err = evaluateFunc(fmt.Sprintf(expr, v), placeholders[i+1:])
			if err != nil {
				return err
			}
		}
		return nil
	}

	if err := evaluateFunc(t.Expr, t.extractPlaceholders()); err != nil {
		return nil, err
	}

	return results, nil
}

// Datasource 数据源类型
type Datasource struct {
	Master string `yaml:"master"`
	Slave  any    `yaml:"slave"`
	config *Config
}

func (d *Datasource) UnmarshalYAML(value *yaml.Node) error {
	type rawDatasource struct {
		Master string         `yaml:"master"`
		Slave  map[string]any `yaml:"slave"`
	}
	var raw rawDatasource
	if err := value.Decode(&raw); err != nil {
		return err
	}
	log.Printf("rawDatasource = %#v\n", raw)
	d.Master = raw.Master
	dataType, err := unmarshalDataType(d.config, "slave", raw.Slave)
	if err != nil {
		return err
	}
	d.Slave = dataType
	return nil
}

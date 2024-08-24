package composite

import (
	"fmt"
	"log"
	"strings"

	"github.com/meoying/dbproxy/config/v2/internal/errs"
	"gopkg.in/yaml.v3"
)

type Ref struct {
	Name    string
	varType string
	// path        string
	Values      map[string]any
	Variables   map[string]any
	Databases   map[string]any
	Datasources map[string]any
}

func (r *Ref) IsZeroValue() bool {
	return len(r.Values) == 0
}

// func (r *Ref) UnmarshalYAML2(value *yaml.Node) error {
// 	var path string
// 	if err := value.Decode(&path); err != nil {
// 		return err
// 	}
// 	if path == "" {
// 		return fmt.Errorf("%w: %q", errs.ErrUnmarshalVariableFailed, r.Name)
// 	}
// 	log.Printf("引用地址 path = %#v\n", path)
// 	v, t, err := r.unmarshalReferencedVariable(strings.Split(path, "."))
// 	if err != nil {
// 		return err
// 	}
// 	log.Printf("获取到的引用类型 v = %#v, t = %s", v, t)
//
// 	// TODO: 再这里解包,还是到具体的类型中?
// 	for range strings.Split(path, ".") {
// 		switch t {
// 		case DataTypeVariable:
// 			vv := v.(Variable)
// 			t = vv.varType
// 			v = vv.Value
// 		case DataTypeDatabase:
// 			vv := v.(Database)
// 			t = vv.varType
// 			v = vv.Value
// 		case DataTypeHash:
// 			vv := v.(Hash)
// 			vv.Name = r.Name
// 			v = vv
// 		case DataTypeTemplate:
// 			vv := v.(Template)
// 			vv.Name = r.Name
// 			v = vv
// 		default:
// 			continue
// 		}
// 	}
// 	log.Printf("准备填入ref v = %#v, t = %s", v, t)
// 	// var refVal any
// 	// switch t {
// 	// case DataTypeHash:
// 	// 	vv := v.(Hash)
// 	// 	vv.varName = r.varName
// 	// 	refVal = vv
// 	// case DataTypeTemplate:
// 	// 	vv := v.(Template)
// 	// 	vv.varName = r.varName
// 	// }
// 	// r.Value = refVal
// 	r.varType = t
// 	r.path = path
// 	r.Values = v
// 	log.Printf("最终的引用类型 ref = %#v\n", r)
// 	return nil
// }

func (r *Ref) UnmarshalYAML(value *yaml.Node) error {
	var paths []string
	if err := value.Decode(&paths); err != nil {
		return err
	}
	if len(paths) == 0 {
		return fmt.Errorf("%w: %q", errs.ErrUnmarshalVariableFailed, r.Name)
	}
	log.Printf("引用地址 paths = %#v\n", paths)

	for _, path := range paths {
		v, t, err := r.unmarshalReferencedVariable(strings.Split(path, "."))
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
				vv.Name = r.Name
				v = vv
			case DataTypeTemplate:
				vv := v.(Template)
				// vv.Name = r.Name
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
		r.Values[path] = v
		log.Printf("最终的引用类型 ref = %#v\n", r)
	}

	return nil
}

func (r *Ref) unmarshalReferencedVariable(paths []string) (any, string, error) {
	var values map[string]any
	var varType string

	switch paths[0] {
	case ConfigFieldVariables:
		log.Printf("config.Variables = %#v\n", r.Variables)
		values = r.Variables
		varType = DataTypeVariable
	case ConfigFieldDatabases:
		log.Printf("config.Databases = %#v\n", r.Databases)
		values = r.Databases
		varType = DataTypeDatabase
	case ConfigSectionDatasources:
		log.Printf("config.Datasources = %#v\n", r.Datasources)
		values = r.Datasources
		varType = DataTypeDatasource
	case ConfigFieldTables:
		return nil, "", fmt.Errorf("%w: 暂不支持对tables的引用", errs.ErrReferencePathInvalid)
	default:
		return nil, "", fmt.Errorf("%w: 未知的小节%s", errs.ErrReferencePathInvalid, paths[0])
	}

	if len(paths) != 2 {
		return nil, "", fmt.Errorf("%w: %s", errs.ErrReferencePathInvalid, strings.Join(paths, "."))
	}

	varName := paths[1]
	// 变量名存在
	v, ok := values[varName]
	if !ok {
		return nil, varType, fmt.Errorf("%w: %s", errs.ErrReferencePathInvalid, strings.Join(paths, "."))
	}

	value, err := UnmarshalUntypedVariable(varType, varName, v)
	if err != nil {
		return nil, varType, err
	}
	values[varName] = value

	return value, varType, nil
}

type Finder[T any] interface {
	Name() string
	Find(name string) (T, error)
}

type Reference[E Placeholder | Datasource | Database | Table, F Finder[E]] struct {
	// global 表示全局预定定义的 Datasources, Databases, Tables, Placeholders
	global Finder[E]
	paths  []string
}

func (r *Reference[E, F]) IsZeroValue() bool {
	return len(r.paths) == 0
}

func (r *Reference[E, F]) UnmarshalYAML(value *yaml.Node) error {
	var paths []string
	err := value.Decode(&paths)
	if err != nil {
		return err
	}
	r.paths = paths
	return nil
}

func (r *Reference[E, F]) Build() (map[string]E, error) {
	mp := make(map[string]E, len(r.paths))
	for _, path := range r.paths {
		varInfo := strings.SplitN(path, ".", 2)
		varType, varName := varInfo[0], varInfo[1]
		log.Printf("引用路径信息 = %#v\n", varInfo)
		t, err := r.global.Find(varName)
		// log.Printf("global = %#v\n", d.global.Variables)
		if varType != r.global.Name() || err != nil {
			return nil, fmt.Errorf("%w: %s", errs.ErrReferencePathInvalid, path)
		}
		mp[varName] = t
	}
	return mp, nil
}

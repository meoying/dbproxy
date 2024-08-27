package composite

import (
	"fmt"
	"log"
	"strings"

	"github.com/meoying/dbproxy/config/v2/internal/errs"
	"gopkg.in/yaml.v3"
)

type Finder[T any] interface {
	Type() string
	Find(name string) (T, error)
}

type Referencable interface {
	Placeholder | Datasource | Database | Table
	TypeName() string
}

type Reference[E Referencable, F Finder[E]] struct {
	// global 表示全局预定定义的 Datasources, Databases, Tables, Placeholders
	global F
	paths  []string
}

func (r *Reference[E, F]) IsZero() bool {
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

// IsReferencedSection 根据 typeName 判断是否引用某个全局小节
func (r *Reference[E, F]) IsReferencedSection(typeName string) bool {
	for _, path := range r.paths {
		if strings.SplitN(path, ".", 2)[0] == typeName {
			return true
		}
	}
	return false
}

func (r *Reference[E, F]) Build() (map[string]E, error) {
	mp := make(map[string]E, len(r.paths))
	for _, path := range r.paths {
		varInfo := strings.SplitN(path, ".", 2)
		varType, varName := varInfo[0], varInfo[1]
		log.Printf("引用路径信息 = %#v\n", varInfo)
		t, err := r.global.Find(varName)
		if varType != r.global.Type() || err != nil {
			return nil, fmt.Errorf("%w: %s", errs.ErrReferencePathInvalid, path)
		}
		mp[varName] = t
	}
	return mp, nil
}

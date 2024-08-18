package config

import (
	"fmt"
	"regexp"
	"strings"
)

type Enum []string

type TemplateType struct {
	Expr         string                 `yaml:"expr"`
	Placeholders map[string]Placeholder `yaml:"placeholders"`
}

type Placeholder struct {
	String  string   `yaml:"string"`
	Enum    Enum     `yaml:"enum,omitempty"`
	Objects []Object `yaml:"objects,omitempty"`
}

type Object struct {
	Key string `yaml:"key"`
}

func (t *TemplateType) Evaluate() ([]string, error) {
	return t.EvaluateWith(nil)
}

// EvaluateWith 方法接受部分占位符值，允许更灵活的使用
func (t *TemplateType) EvaluateWith(partialPlaceholders map[string][]interface{}) ([]string, error) {
	var results []string
	err := t.evaluate(t.Expr, t.Placeholders, partialPlaceholders, &results)
	return results, err
}

func (t *TemplateType) evaluate(expr string, placeholders map[string]Placeholder, partialPlaceholders map[string][]interface{}, results *[]string) error {
	re := regexp.MustCompile(`\$\{([^}]+)\}`)
	matches := re.FindAllStringSubmatch(expr, -1)

	if len(matches) == 0 {
		*results = append(*results, expr)
		return nil
	}

	for _, match := range matches {
		fullMatch := match[0]
		innerExpr := match[1]
		placeholderName := strings.TrimSpace(innerExpr)
		placeholderName = strings.TrimRight(placeholderName, ".")

		if placeholder, ok := placeholders[placeholderName]; ok {
			var values []interface{}

			// 首先检查是否有部分占位符值
			if partialValues, exists := partialPlaceholders[placeholderName]; exists && len(partialValues) > 0 {
				values = partialValues
			} else {
				// 如果没有部分占位符值，则使用完整的占位符定义
				if placeholder.String != "" {
					values = []interface{}{placeholder.String}
				} else if len(placeholder.Enum) > 0 {
					for _, v := range placeholder.Enum {
						values = append(values, v)
					}
				} else if len(placeholder.Objects) > 0 {
					for _, obj := range placeholder.Objects {
						values = append(values, obj.Key)
					}
				}
			}

			if len(values) == 0 {
				values = []interface{}{""}
			}

			for _, value := range values {
				newExpr := expr
				strValue := fmt.Sprintf("%v", value)
				if strValue == "" {
					newExpr = strings.Replace(newExpr, fullMatch, strings.TrimLeft(innerExpr, placeholderName), 1)
				} else {
					replacement := strValue
					if strings.HasSuffix(innerExpr, ".") && strValue != "" {
						replacement += "."
					}
					newExpr = strings.Replace(newExpr, fullMatch, replacement, 1)
				}

				newPlaceholders := make(map[string]Placeholder)
				for k, v := range placeholders {
					if k != placeholderName {
						newPlaceholders[k] = v
					}
				}

				if err := t.evaluate(newExpr, newPlaceholders, partialPlaceholders, results); err != nil {
					return err
				}
			}
			return nil
		}
	}

	if re.MatchString(expr) {
		return fmt.Errorf("未解析的占位符在表达式中: %s", expr)
	}

	*results = append(*results, expr)
	return nil
}

// func (t *TemplateType) Search(placeholders map[string]string) ([]string, error) {
// 	if len(placeholders) == 0 {
// 		return t.Evaluate()
// 	}
// 	return t.EvaluateWith(placeholders)
// }

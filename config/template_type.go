package config

import (
	"fmt"
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

func (t *TemplateType) EvaluateWith(partialPlaceholders map[string]string) ([]string, error) {
	var results []string
	err := t.evaluate(t.Expr, t.Placeholders, partialPlaceholders, &results)
	return results, err
}

func (t *TemplateType) evaluate(expr string, placeholders map[string]Placeholder, partialPlaceholders map[string]string, results *[]string) error {
	if len(placeholders) == 0 {
		*results = append(*results, expr)
		return nil
	}

	for key, placeholder := range placeholders {
		if value, ok := partialPlaceholders[key]; ok {
			newExpr := strings.Replace(expr, "${"+key+"}", value, -1)
			newPlaceholders := make(map[string]Placeholder)
			for k, v := range placeholders {
				if k != key {
					newPlaceholders[k] = v
				}
			}
			return t.evaluate(newExpr, newPlaceholders, partialPlaceholders, results)
		}

		if len(placeholder.Enum) > 0 {
			for _, value := range placeholder.Enum {
				newExpr := strings.Replace(expr, "${"+key+"}", value, -1)
				newPlaceholders := make(map[string]Placeholder)
				for k, v := range placeholders {
					if k != key {
						newPlaceholders[k] = v
					}
				}
				if err := t.evaluate(newExpr, newPlaceholders, partialPlaceholders, results); err != nil {
					return err
				}
			}
			return nil
		} else if len(placeholder.Objects) > 0 {
			for _, obj := range placeholder.Objects {
				value := obj.Key
				if value == "" {
					value = ""
				} else {
					value = value + "."
				}
				newExpr := strings.Replace(expr, "${"+key+"}", value, -1)
				newPlaceholders := make(map[string]Placeholder)
				for k, v := range placeholders {
					if k != key {
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
	return fmt.Errorf("no valid placeholders found for expression: %s", expr)
}

// func (t *TemplateType) evaluate(expr string, placeholders map[string]Placeholder, partialPlaceholders map[string]string, results *[]string) error {
// 	if len(placeholders) == 0 {
// 		*results = append(*results, expr)
// 		return nil
// 	}
//
// 	for key, placeholder := range placeholders {
// 		if value, ok := partialPlaceholders[key]; ok {
// 			newExpr := strings.Replace(expr, "${"+key+"}", value, -1)
// 			newPlaceholders := make(map[string]Placeholder)
// 			for k, v := range placeholders {
// 				if k != key {
// 					newPlaceholders[k] = v
// 				}
// 			}
// 			t.evaluate(newExpr, newPlaceholders, partialPlaceholders, results)
// 			return nil
// 		}
//
// 		if len(placeholder.Enum) > 0 {
// 			for _, value := range placeholder.Enum {
// 				newExpr := strings.Replace(expr, "${"+key+"}", value, -1)
// 				newPlaceholders := make(map[string]Placeholder)
// 				for k, v := range placeholders {
// 					if k != key {
// 						newPlaceholders[k] = v
// 					}
// 				}
// 				t.evaluate(newExpr, newPlaceholders, partialPlaceholders, results)
// 			}
// 		} else if len(placeholder.Objects) > 0 {
// 			for _, obj := range placeholder.Objects {
// 				value := obj.Key
// 				if value == "" {
// 					value = ""
// 				} else {
// 					value = value + "."
// 				}
// 				newExpr := strings.Replace(expr, "${"+key+"}", value, -1)
// 				newPlaceholders := make(map[string]Placeholder)
// 				for k, v := range placeholders {
// 					if k != key {
// 						newPlaceholders[k] = v
// 					}
// 				}
// 				t.evaluate(newExpr, newPlaceholders, partialPlaceholders, results)
// 			}
// 		}
// 		return nil
// 	}
// 	return fmt.Errorf("no valid placeholders found")
// }

func (t *TemplateType) Search(placeholders map[string]string) ([]string, error) {
	if len(placeholders) == 0 {
		return t.Evaluate()
	}
	return t.EvaluateWith(placeholders)
}

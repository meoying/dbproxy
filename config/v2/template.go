package v2

import (
	"fmt"
	"log"
	"regexp"
	"strings"

	"gopkg.in/yaml.v3"
)

// Template 模版类型
type Template struct {
	global       *Section[Placeholder]
	Expr         string               `yaml:"expr"`
	Placeholders Section[Placeholder] `yaml:"placeholders"`
}

func (t *Template) IsZero() bool {
	return t.Expr == "" && t.Placeholders.IsZero()
}

func (t *Template) UnmarshalYAML(value *yaml.Node) error {
	type rawTemplate struct {
		Expr         string               `yaml:"expr"`
		Placeholders Section[Placeholder] `yaml:"placeholders"`
	}
	raw := rawTemplate{
		Placeholders: *NewSection[Placeholder](ConfigSectionTypePlaceholders, t.global, nil, NewPlaceholder),
	}
	log.Printf("before raw.Template = %#v\n", raw)
	if err := value.Decode(&raw); err != nil {
		return err
	}

	log.Printf("after raw.Template = %#v\n", raw)

	t.Expr = strings.TrimSpace(raw.Expr)
	if len(t.Expr) == 0 {
		return fmt.Errorf("%w: template.expr", ErrUnmarshalVariableFailed)
	}

	if raw.Placeholders.IsZero() {
		return fmt.Errorf("%w: template.placeholders", ErrUnmarshalVariableFailed)
	}

	for _, ph := range t.placeholdersInExpr() {
		if _, ok := raw.Placeholders.Variables[ph]; !ok {
			return fmt.Errorf("%w: template.placeholders 缺少占位符 %s", ErrUnmarshalVariableFailed, ph)
		}
	}

	for ph := range raw.Placeholders.Variables {
		if !strings.Contains(t.Expr, fmt.Sprintf("${%s}", ph)) {
			return fmt.Errorf("%w: template.expr 缺少占位符 %s", ErrUnmarshalVariableFailed, ph)
		}
	}

	t.Placeholders = raw.Placeholders
	return nil
}

func (t *Template) placeholdersInExpr() []string {
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

func (t *Template) Evaluate() (map[string]string, error) {
	results := make(map[string]string)
	keys := make([]string, 0, len(t.Placeholders.Variables))

	if err := t.evaluate(results, t.Expr, t.placeholdersInExpr(), &keys); err != nil {
		return nil, err
	}
	return results, nil
}

func (t *Template) evaluate(results map[string]string, expr string, placeholders []string, keys *[]string) error {
	if len(placeholders) == 0 {
		results[strings.Join(*keys, "_")] = expr
		log.Printf("evaluate(res) = %#v\n", results)
		return nil
	}

	i := 0
	ph := placeholders[i]
	expr = strings.Replace(expr, "${"+ph+"}", "%s", 1)

	placeholder := t.Placeholders.Variables[ph]
	values, err := placeholder.Evaluator().Evaluate()
	if err != nil {
		return err
	}

	n := len(*keys)
	for _, v := range values {
		*keys = (*keys)[:n]
		*keys = append(*keys, v)
		err = t.evaluate(results, fmt.Sprintf(expr, v), placeholders[i+1:], keys)
		if err != nil {
			return err
		}
	}
	return nil
}

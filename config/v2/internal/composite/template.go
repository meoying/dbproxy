package composite

import (
	"fmt"
	"log"
	"regexp"
	"strings"

	"github.com/meoying/dbproxy/config/v2/internal/errs"
	"gopkg.in/yaml.v3"
)

// Template 模版类型
type Template struct {
	global       *Placeholders
	Expr         string       `yaml:"expr"`
	Placeholders Placeholders `yaml:"placeholders"`
}

func (t *Template) IsZeroValue() bool {
	return t.Expr == "" && t.Placeholders.IsZeroValue()
}

func (t *Template) UnmarshalYAML(value *yaml.Node) error {
	type rawTemplate struct {
		Expr         string       `yaml:"expr"`
		Placeholders Placeholders `yaml:"placeholders"`
	}
	raw := rawTemplate{
		Placeholders: Placeholders{
			global: t.global,
		},
	}
	if err := value.Decode(&raw); err != nil {
		return err
	}

	log.Printf("raw.Template = %#v\n", raw)

	t.Expr = strings.TrimSpace(raw.Expr)
	if len(t.Expr) == 0 {
		return fmt.Errorf("%w: template.expr", errs.ErrUnmarshalVariableFailed)
	}

	if raw.Placeholders.IsZeroValue() {
		return fmt.Errorf("%w: template.placeholders", errs.ErrUnmarshalVariableFailed)
	}

	for _, ph := range t.placeholdersInExpr() {
		if _, ok := raw.Placeholders.variables[ph]; !ok {
			return fmt.Errorf("%w: template.placeholders 缺少占位符 %s", errs.ErrUnmarshalVariableFailed, ph)
		}
	}

	for ph := range raw.Placeholders.variables {
		if !strings.Contains(t.Expr, fmt.Sprintf("${%s}", ph)) {
			return fmt.Errorf("%w: template.expr 缺少占位符 %s", errs.ErrUnmarshalVariableFailed, ph)
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
	keys := make([]string, 0, len(t.Placeholders.variables))

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

	placeholder := t.Placeholders.variables[ph]
	values, err := placeholder.Value().Evaluate()
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

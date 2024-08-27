package v2

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestUnmarshal_Datasource(t *testing.T) {
	tests := []struct {
		name      string
		ph        *Section[Placeholder]
		val       any
		want      any
		assertErr assert.ErrorAssertionFunc
	}{
		{
			name: "datasources模版",
			val: map[string]any{
				"ds_template": map[string]any{
					"master": "master.${key}",
					"slaves": "slaves.${key}",
					"placeholders": map[string]any{
						"key": any("0"),
					},
				},
			},
			want: DatasourceTemplate{
				Master: Template{
					Expr: "master.${key}",
					Placeholders: Section[Placeholder]{
						Variables: map[string]Placeholder{
							"key": {
								Value: String("0"),
							},
						},
					},
				},
				Slaves: Template{
					Expr: "slaves.${key}",
					Placeholders: Section[Placeholder]{
						Variables: map[string]Placeholder{
							"key": {
								Value: String("0"),
							},
						},
					},
				},
			},
			assertErr: assert.NoError,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := unmarshal[Datasource, *Section[Datasource]](tt.ph, tt.val)
			tt.assertErr(t, err)
			if err != nil {
				return
			}
			assert.EqualExportedValues(t, tt.want, got)
		})
	}
}

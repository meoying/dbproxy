package config_test

import (
	"testing"

	"github.com/meoying/dbproxy/config"
	"github.com/stretchr/testify/assert"
)

func TestParseConfig(t *testing.T) {
	t.Skip()
	tests := []struct {
		name    string
		content string

		expected     *config.Config
		asserErrFunc assert.ErrorAssertionFunc
	}{
		{
			name:         "配置文件合法_空文件",
			content:      ``,
			expected:     &config.Config{},
			asserErrFunc: assert.NoError,
		},
		{
			name: "配置文件合法_仅含分片键规则_模版类型",
			content: `
shardingKeys:
  db_key:
    template:
      expr: "${key}/10%2"
      placeholders:
        key:
          string: "user_id"`,
			expected: &config.Config{
				ShardingKeys: map[string]config.ShardingKey{
					"db_key": {
						Template: config.TemplateType{
							Expr: "${key}/10%2",
							Placeholders: map[string]config.Placeholder{
								"key": {
									String: "user_id",
								},
								// "key": {Value: "user_id"},
							},
						},
					},
				},
			},
			asserErrFunc: assert.NoError,
		},
		{
			name: "配置文件合法_含分片键和分库规则字符串类型",
			content: `
databases:
  user_db:
    string: "user_db"
  order_db:
    template:
      expr: "order_db_${key}"
      placeholders:
        key:
          string: "user_id"

shardingKeys:
  db_key:
    template:
      expr: "${key}/10%2"
      placeholders:
        key:
          string: "user_id"`,
			expected: &config.Config{
				ShardingKeys: map[string]config.ShardingKey{
					"db_key": {
						Template: config.TemplateType{
							Expr: "${key}/10%2",
							Placeholders: map[string]config.Placeholder{
								"key": {
									String: "user_id",
								},
								// "key": {Value: "user_id"},
							},
						},
					},
				},
			},
			asserErrFunc: assert.NoError,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			actual, err := config.ParseContent(tt.content)
			tt.asserErrFunc(t, err)
			if err != nil {
				return
			}
			assert.Equal(t, tt.expected, actual)
		})
	}
}

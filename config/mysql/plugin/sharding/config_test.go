package sharding

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

func TestConfig(t *testing.T) {
	yamlData, err := os.ReadFile("testdata/config/config.yaml")
	require.NoError(t, err)

	var config Config
	err = yaml.Unmarshal(yamlData, &config)
	require.NoError(t, err)

	expectedConfig := Config{
		Algorithm: Algorithm{
			Hash: &Hash{
				ShardingKey: "user_id",
				DSPattern:   Pattern{Base: 0, Name: "0.db.cluster.company.com:3306", NotSharding: true},
				DBPattern:   Pattern{Base: 3, Name: "driver_db_%d", NotSharding: false},
				TBPattern:   Pattern{Base: 0, Name: "order_tab", NotSharding: true},
			},
		},
		Datasource: Datasource{
			Clusters: []Cluster{
				{
					Address: "0.db.cluster.company.com:3306",
					Nodes: []Nodes{
						{
							Master: DSNConfig{Name: "driver_db_0", DSN: "root:root@tcp(127.0.0.1:13306)/driver_db_0?charset=utf8mb4&parseTime=True&loc=Local"},
							Slaves: []*DSNConfig{
								{Name: "slave-01", DSN: "root:root@tcp(127.0.0.1:13306)/driver_db_0?charset=utf8mb4&parseTime=True&loc=Local"},
							},
						},
						{
							Master: DSNConfig{Name: "driver_db_1", DSN: "root:root@tcp(127.0.0.1:13306)/driver_db_1?charset=utf8mb4&parseTime=True&loc=Local"},
							Slaves: nil,
						},
						{
							Master: DSNConfig{Name: "driver_db_2", DSN: "root:root@tcp(127.0.0.1:13306)/driver_db_2?charset=utf8mb4&parseTime=True&loc=Local"},
							Slaves: nil,
						},
					},
				},
			},
		},
	}
	assert.Equal(t, expectedConfig, config)
}

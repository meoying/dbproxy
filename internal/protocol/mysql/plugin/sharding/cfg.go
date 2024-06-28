package sharding

// sharding插件的配置文件
type Config struct {
	// 分库分表的配置
	DBPattern    string `json:"db_pattern"`
	TablePattern string `json:"table_pattern"`
	DSPattern    string `json:"dsp_pattern"`
	DBBase       int    `json:"db_base"`
	DSBase       int    `json:"ds_base"`
	TableBase    int    `json:"table_base"`
	ShardingKey  string `json:"sharding_key"`
	// 真正连接数据库的dsn
	DBDsns []string `json:"db_dsns"`
}

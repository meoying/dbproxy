package sharding

type Config struct {
	Algorithm  Algorithm  `yaml:"algorithm"`
	Datasource Datasource `yaml:"datasource"`
}

type Algorithm struct {
	Hash *Hash `yaml:"hash"`
}

type Datasource struct {
	Clusters []Cluster `yaml:"clusters"`
}

type Hash struct {
	ShardingKey string `yaml:"shardingKey"`
	// 分集群
	DSPattern Pattern `yaml:"dsPattern"`
	// 分库
	DBPattern Pattern `yaml:"dbPattern"`
	// 分表
	TBPattern Pattern `yaml:"tbPattern"`
}

type Pattern struct {
	Base        int    `yaml:"base"`
	Name        string `yaml:"name"`
	NotSharding bool   `yaml:"notSharding"`
}

type Cluster struct {
	Address string  `yaml:"address"`
	Nodes   []Nodes `yaml:"nodes"`
}

type Nodes struct {
	Master DSNConfig    `yaml:"master"`
	Slaves []*DSNConfig `yaml:"slaves,omitempty"`
}

type DSNConfig struct {
	Name string `yaml:"name"`
	DSN  string `yaml:"dsn"`
}

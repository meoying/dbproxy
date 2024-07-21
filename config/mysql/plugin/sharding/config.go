package sharding

type Config struct {
	Algorithm  Algorithm  `json:"algorithm" yaml:"algorithm"`
	Datasource Datasource `json:"datasource" yaml:"datasource"`
}

type Algorithm struct {
	Hash *Hash `json:"hash" yaml:"hash"`
}

type Datasource struct {
	Clusters []Cluster `json:"clusters" yaml:"clusters"`
}

type Hash struct {
	ShardingKey string `json:"shardingKey" yaml:"shardingKey"`
	// 分集群
	DSPattern Pattern `json:"dsPattern" yaml:"dsPattern"`
	// 分库
	DBPattern Pattern `json:"dbPattern" yaml:"dbPattern"`
	// 分表
	TBPattern Pattern `json:"tbPattern" yaml:"tbPattern"`
}

type Pattern struct {
	Base        int    `json:"base" yaml:"base"`
	Name        string `json:"name" yaml:"name"`
	NotSharding bool   `json:"notSharding" yaml:"notSharding"`
}

type Cluster struct {
	Address string  `json:"address" yaml:"address"`
	Nodes   []Nodes `json:"nodes" yaml:"nodes"`
}

type Nodes struct {
	Master DSNConfig    `json:"master" yaml:"master"`
	Slaves []*DSNConfig `json:"slaves,omitempty" yaml:"slaves,omitempty"`
}

type DSNConfig struct {
	Name string `json:"name" yaml:"name"`
	DSN  string `json:"dsn" yaml:"dsn"`
}

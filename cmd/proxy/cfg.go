package main

type Config struct {
	Addr    string            `yaml:"addr"`
	Plugins map[string]Plugin `yaml:"plugins"`
}

type Plugin struct {
	Name string         `yaml:"name"`
	Cfg  map[string]any `yaml:"cfg"`
}

package log

// Config 定义日志配置的结构
type Config struct {
	Level  string `json:"level" yaml:"level"`
	Output string `json:"output" yaml:"output"`
}

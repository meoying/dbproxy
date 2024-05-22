package main

import (
	"fmt"

	"github.com/meoying/dbproxy/internal/protocol/mysql"
	"github.com/meoying/dbproxy/internal/protocol/mysql/plugin"
	"github.com/meoying/dbproxy/internal/protocol/mysql/plugin/forward"
	"github.com/meoying/dbproxy/internal/protocol/mysql/plugin/log"
	"github.com/spf13/pflag"
	"github.com/spf13/viper"
)

func main() {
	cfile := pflag.String("config",
		"config/config.yaml", "配置文件路径")
	viper.SetConfigType("yaml")

	viper.SetConfigFile(*cfile)
	err := viper.ReadInConfig()
	if err != nil {
		panic(fmt.Errorf("初始化读取配置文件失败 %w", err))
	}
	var cfg Config
	err = viper.Unmarshal(&cfg)
	if err != nil {
		panic(fmt.Errorf("解析配置文件失败 %w", err))
	}
	// TODO 加载 .so 来完成
	plugins := []plugin.Plugin{
		&forward.Plugin{},
		&log.Plugin{},
	}

	server := mysql.NewServer(cfg.Addr, plugins)
	err = server.Start()
	if err != nil {
		// 可以是正常退出，也可能是异常退出，暂时还没区分并且解决
		panic(err)
	}
}

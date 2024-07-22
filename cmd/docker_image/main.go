package main

import (
	"encoding/json"
	"fmt"
	"log"
	"strings"

	"github.com/ecodeclub/ekit/spi"
	"github.com/spf13/viper"

	"github.com/meoying/dbproxy/internal/protocol/mysql"
	"github.com/meoying/dbproxy/internal/protocol/mysql/plugin"
)

const (
	pluginLocationTmpl = "./plugins/%s"
	pluginConfigTmpl   = "./plugins/%s/config.yaml"
)

func main() {

	viper.SetConfigType("yaml")
	viper.SetConfigFile("config.yaml")

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
	var plugins []plugin.Plugin
	for _, p := range cfg.Plugins.Items {
		// 加载配置文件
		configFile := fmt.Sprintf(pluginConfigTmpl, strings.ToLower(p.Name))
		viper.SetConfigFile(configFile)
		err = viper.ReadInConfig()
		if err != nil {
			panic(fmt.Errorf("解析配置文件失败 %w", err))
		}
		configData := make(map[string]any, 16)
		err = viper.Unmarshal(&configData)
		if err != nil {
			panic(fmt.Errorf("解析配置文件失败 %w", err))
		}
		configByte, err := json.Marshal(configData)
		if err != nil {
			panic(fmt.Errorf("解析配置文件失败 %w", err))
		}
		// 加载插件
		ps, err := spi.LoadService[plugin.Plugin](fmt.Sprintf(pluginLocationTmpl, strings.ToLower(p.Name)), "Plugin")
		if err != nil {
			panic(fmt.Errorf("加载插件失败 %w", err))
		}
		// 初始化插件
		err = ps[0].Init(configByte)
		if err != nil {
			panic(fmt.Errorf("初始化插件失败 %w", err))
		}
		log.Printf("初始化 %s 插件成功......", ps[0].Name())
		plugins = append(plugins, ps[0])
	}
	server := mysql.NewServer(cfg.Server.Addr, plugins)
	log.Printf("服务开启。。。。端口：%s", cfg.Server.Addr)
	err = server.Start()
	if err != nil {
		// 可以是正常退出，也可能是异常退出，暂时还没区分并且解决
		panic(err)
	}
}

type Config struct {
	Server  Server  `yaml:"server"`
	Plugins Plugins `yaml:"plugins"`
}

type Server struct {
	Addr string `yaml:"addr"`
}

type Plugins struct {
	Location string   `yaml:"location"`
	Items    []Plugin `yaml:"items"`
}

type Plugin struct {
	Name           string `yaml:"name"`
	Location       string `yaml:"location"`
	ConfigLocation string `yaml:"config_location"`
}

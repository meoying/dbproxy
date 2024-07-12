//go:build e2e

package test

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/spf13/viper"
)

func getConfig(path string) ([]byte, error) {
	dir, err := os.Getwd()
	if err != nil {
		return nil, err
	}
	viper.SetConfigFile(filepath.Clean(fmt.Sprintf("%s/%s", dir, path)))
	err = viper.ReadInConfig()
	if err != nil {
		return nil, err
	}
	configData := make(map[string]any, 16)
	err = viper.Unmarshal(&configData)
	if err != nil {
		return nil, err
	}
	return json.Marshal(configData)
}

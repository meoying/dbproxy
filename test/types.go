//go:build e2e

package test

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/spf13/viper"
)

func getAbsPath(path string) (string, error) {
	dir, err := os.Getwd()
	if err != nil {
		return "", err
	}
	return filepath.Clean(fmt.Sprintf("%s/%s", dir, path)), nil
}

func unmarshalConfigFile(path string) ([]byte, error) {
	absPath, err := getAbsPath(path)
	if err != nil {
		return nil, err
	}
	viper.SetConfigFile(absPath)
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

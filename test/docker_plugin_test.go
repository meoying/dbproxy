//go:build e2e

package test

import "testing"

// TestDockerDBProxy 测试Docker形态的dbproxy
func TestDockerDBProxy(t *testing.T) {
	t.Skip()

	t.Run("TestForwardSuite", func(t *testing.T) {
		// docker镜像中指定forward
	})

	t.Run("TestShardingSuite", func(t *testing.T) {
		// 在Docker镜像中指定sharding配置
	})
}

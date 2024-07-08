## 项目结构
mysql -- 保存mysql的测试deployment方便部署，具体查看项目下的README

sidecar -- 保存sidecar的部署形态所使用的文件，具体查看项目下的README

sidecar_container -- 保存sidecar_container的部署形态所使用的文件具体查看项目下的README

### testserver
项目结构
```shell
.
├── Dockerfile
├── etc
│   └── config.yaml
├── go.mod
├── go.sum
└── main.go
```
较为重要的 config.yaml
```shell
db:
  dsn: "root:root@tcp(localhost:8307)/?interpolateParams=true"
```
现在无论是哪种形式的部署形态都是基于sidecar形式，也就是dbproxy和应用容器，共享网络空间，所以配置的地址就是localhost + 端口。

# cmd/docker_image用于构建Docker镜像

## 容器内部目录说明

应用在容器内部的目录说明:
1. /app/dbproxy/目录下dbproxy为可执行二进制
2. /app/dbproxy/config.yaml为dbproxy二进制文件的**主配置文件**
3. /app/dbproxy/plugins/$name.so是dbproxy二进制文件支持的插件名,$name=log|forward|sharding
4. /app/dbproxy/plugins/$name/config.yaml是$name插件的**插件配置文件**

## 启动dbproxy容器前的准备工作

- 准备dbproxy主配置文件`dbproxy.yaml`

```yaml
# 代理 server 有关的配置
server:
  # 服务器启动监听的端口
  addr: ":8308"
# 使用的插件的配置，我们会按照插件的顺序进行加载和初始化
plugins:
  items:
    - name: "log"
    - name: "forward"
```

- 准备dbproxy中开启的插件配置文件`log.yaml`和`forward.yaml`

```yaml
# log.yaml
# 当前为空
```
```yaml
# forward.yaml
dsn: "root:root@tcp(127.0.0.1:3306)/order_db?charset=utf8mb4&parseTime=True&loc=Local"
name: dbproxy
``` 

- 安装docker, 并执行启动容器命令

```shell
docker run -it -p 8038:8038 \
          -v $(pwd)/test/testdata/config/docker/dbproxy.yaml:/app/dbproxy/config.yaml \
          -v $(pwd)/test/testdata/config/docker/plugins/log.yaml:/app/dbproxy/plugins/log/config.yaml \
          -v $(pwd)/test/testdata/config/docker/plugins/forward.yaml:/app/dbproxy/plugins/forward/config.yaml \
          --name mydbproxy flycash/dbproxy:dbproxy-v0.3

# 注意:
# 主配置文件dbproxy.yaml中开启了哪些插件,比如log、forward插件,那么就要提供log、forward插件的配置文件,并且配置文件的映射路径必须满足上方“容器内部目录说明”
```

## 镜像构建指导

1. 提升版本, 将`/dbproxy/Makefile`中的`IMAGE_VERSION`增加1
2. 登录Docker账号, `docker login -u username -p password`
3. 构建镜像, 执行`make build_docker_image`
4. 启动镜像, 检查`/dbproxy/.script/integration_test_compose.yml`中的dbproxy.image及dbproxy.volumes是否符合预期
5. 测试镜像, 执行`make e2e`
6. 推送镜像, 执行`make push_docker_image`

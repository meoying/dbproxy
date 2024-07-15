# 用于构建Docker镜像

## 容器内部目录说明

应用在容器内部的目录说明:
1. /app/dbproxy/目录下dbproxy为可执行二进制
2. /app/dbproxy/config.yaml为dbproxy二进制文件的主配置文件
3. /app/dbproxy/plugins/$name.so是dbproxy支持的插件,%name=log|forward|sharding
4. /app/dbproxy/plugins/$name/config.yaml是$name插件的配置文件

## 启动容器

1. 准备dbproxy主配置文件`dbproxy.yaml`
```yaml
# 代理 server 有关的配置
server:
  # 服务器启动监听的端口
  addr: ":8308"
# 使用的插件的配置，我们会按照插件的顺序进行加载和初始化
plugins:
  items:
    - name: "accessLog"
    - name: "forward"
```

2. 准备dbproxy中开启的插件配置文件`log.yaml`和`forward.yaml`

```yaml
# log.yaml
# 当前为空
```
```yaml
# forward.yaml
dsn: "root:root@tcp(127.0.0.1:3306)/order_db?charset=utf8mb4&parseTime=True&loc=Local"
``` 
3.执行启动命令

```shell
docker run -it -p 8038:8038 \
          -v $(pwd)/test/testdata/config/docker/dbproxy.yaml:/app/dbproxy/config.yaml \
          -v $(pwd)/test/testdata/config/docker/plugins/log.yaml:/app/dbproxy/plugins/log/config.yaml \
          -v $(pwd)/test/testdata/config/docker/plugins/forward.yaml:/app/dbproxy/plugins/forward/config.yaml \
          --name mydbproxy flycash/dbproxy:dbproxy-v0.7
```

- 主config.yaml中开启哪些插件就要提供哪些插件的配置文件,配置目录必须复合“容器内部目录说明”

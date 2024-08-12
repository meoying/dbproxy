# 使用Sidecar模式部署dbproxy

## 项目介绍

app是一个简单的CRUD后端项目,其目录结构及介绍如下:

```shell
├── Dockerfile # 构建app镜像
├── Makefile  # 常用命令
├── README.md
├── config
│   ├── config.yaml # app 的配置文件
│   ├── dbproxy-config.yaml # dbproxy的主配置文件
│   ├── dbproxy-plugin-forward-config.yaml # dbproxy的forward插件配置文件
│   └── dbproxy-plugin-log-config.yaml # dbproxy的log插件配置文件
├── deployments
│   ├── sidecar # 传统sidecar模式声明文件
│   │   ├── app.yaml
│   │   └── mysql.yaml
│   └── sidecar_container # 1.29新特性sidecar容器声明文件
│       ├── app.yaml
│       └── mysql.yaml
├── go.mod
├── go.sum
├── main.go # 应用代码
└── scripts
    └── mysql
        └── init.sql # 数据库表定义

```

## 搭建环境

1. 安装[kind](https://kind.sigs.k8s.io/docs/user/quick-start/#installation),要求k8s镜像至少为v1.29.[参考kind配置](../kind-config.yaml)
2. 安装[kubectl](https://kubernetes.io/docs/tasks/tools/#kubectl),其版本要与k8s版本匹配,可通过`kubectl version --output=yaml`查看.
3. 项目本身提供了一些命令方便您快速搭建环境,详见[kind.mk](../kind.mk)及[Makefile](./Makefile)


## 部署应用

### 传统Sidecar部署模式

传统的Sidecar部署模式就是将app与dbproxy部署在同一个pod中, 将mysql部署在另一个pod中. 这也意味着app与dbproxy之间是共享网络的, 即在app的[config.yaml](./config/config.yaml)中
你可以用`localhost:port`来访问dbproxy,其中port为[dbproxy-config.yaml](./config/dbproxy-config.yaml)中的`server.addr`. 

关于Sidecar部署模式更加详尽的解释请参考官方文档[Kubernetes v1.28: Introducing native sidecar containers](https://kubernetes.io/blog/2023/08/25/native-sidecar-containers/),下
面我们开始以sidecar模式部署应用:,具体声明文件详见[/deployments/sidecar/app.yaml](./deployments/sidecar/app.yaml)文件(提供了注释解释):

```shell

# 启动k8s集群,会执行一系列的操作最终构建出一个可用k8s集群
make k8s_up

# 以传统sidecar模式部署应用
make deploy_sidecar

# 查看 pod 运行状态
kubectl get pods -n sidecar

NAME                                READY   STATUS    RESTARTS      AGE
app-sidecar-6dccfc99db-7slxm        2/2     Running   1 (13s ago)   14s
mysql-deployment-646865fc5b-b2hb2   1/1     Running   0             14s

# 查看容器运行日志
kubectl logs app-sidecar-6dccfc99db-7slxm -n sidecar -c app-sidecar
kubectl logs app-sidecar-6dccfc99db-7slxm -n sidecar -c dbproxy-sidecar
```

参照下方[动手验证](#动手验证)小节来验证dbproxy是否正常工作

### Sidecar容器模式

上方传统Sidecar部署模式有一些明显的缺陷比如启动顺序不确定等,但因该部署模式被广泛使用应所以K8s官方在v1.28将其内置为一个实验新特性——详见[Sidecar Container](https://kubernetes.io/docs/concepts/workloads/pods/sidecar-containers/)

K8s官方在设计该特性之初就考虑到了迁移成本,所以设计的非常巧妙 —— 用户只需稍稍改动一下原有声明文件即可使用新特性完成迁移. 
完整声明文件详见[./deployments/sidecar_container/app.yaml](./deployments/sidecar_container/app.yaml)(包含关键注释),下面我们开始以sidecar container模式部署应用:

```shell
# 启动k8s集群 会执行一系列的操作最终构建出一个可用k8s集群
make k8s_up

# 以sidecar container模式部署应用
make deploy_sidecarcontainer

# 查看 pod 运行状态
kubectl get pods -n sidecar

NAME                                READY   STATUS    RESTARTS      AGE
app-sidecar-5ffd8488b7-m4gd7        2/2     Running   1 (22s ago)   23s
mysql-deployment-646865fc5b-x5rtb   1/1     Running   0             23s

# 查看容器运行日志
kubectl logs app-sidecar-5ffd8488b7-m4gd7 -n sidecar -c app-sidecar
kubectl logs app-sidecar-5ffd8488b7-m4gd7 -n sidecar -c dbproxy-sidecar

```

参照下方[动手验证](#动手验证)小节来验证dbproxy是否正常工作

## 动手验证

使用kind部署的K8s集群其集群节点其实是一个容器. 因此在本地验证时需要先进入节点容器的内部再执行验证操作.

```shell
# 进入集群节点内部
docker exec -it dbproxy-example-control-plane /bin/bash
```

注意: 如果K8s集群是在本地物理机部署,下方链接中的域名可以用localhost替换;如果是云主机部署,可以用云主机的公网IP替换.

### 插入数据

```shell
curl http://dbproxy-example-worker:30080/order \
     -H "Content-Type: application/json" \
     -d '{
        "userId": 1,
        "orderId": 2,
        "content": "app",
        "amount": 1.1
     }'
```
响应数据: `{"message":"Order created successfully"}`

### 获取数据

```shell
curl http://dbproxy-example-worker:30080/order/2 
```
响应数据: `{"orderId":2,"userId":1,"content":"app","amount":1.1}`

### 修改数据

```shell
curl -X PUT http://dbproxy-example-worker:30080/order/2 \
     -H "Content-Type: application/json" \
     -d '{
         "orderId": 2,
         "userId": 1001,
         "content": "Updated order content",
         "amount": 199.99
     }'
```
响应数据: `{"message":"Order updated successfully"}`

再次查询数据得到响应: `{"orderId":2,"userId":1001,"content":"Updated order content","amount":199.99}`

### 删除数据

```shell
curl -X DELETE http://dbproxy-example-worker:30080/order/2
```

响应数据: `{"message":"Order deleted successfully"}`

再次查询数据得到响应: `{"error":"Order not found"}`
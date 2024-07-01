


## 运行
```shell
加载配置文件
kubectl apply -f shardingConfig.yaml
kubectl apply -f config.yaml
kubectl create configmap  mysql-init-script-configmap --from-file=init.sql
运行deployment
kubectl apply -f dbproxy.yaml

查看现象
在node节点上访问
curl --location '127.0.0.1:8080/order' \
--header 'Content-Type: application/json' \
--data '{
    "userId": 1,
    "orderId": 2,
    "content": "jiji",
    "account": 1.1
}'

```

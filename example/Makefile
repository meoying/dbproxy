# 定义变量
DBPROXY_IMAGE := flycash/dbproxy:dbproxy-v0.1
TEST_SERVER_IMAGE := flycash/dbproxy:testserver-v0.1
MYSQL_IMAGE := mysql:8.0.29

.PHONY: pull_docker_images
pull_docker_images:
	@docker pull $(DBPROXY_IMAGE)
	@docker pull $(TESTSERVER_IMAGE)
	@docker pull $(MYSQL_IMAGE)

.PHONY: create_cluster
create:
	@kind create cluster --name dbproxy-example --config ./kind-config.yaml

.PHONY: delete_cluster
delete:
	@kind delete cluster --name dbproxy-example

.PHONY: load_docker_images
load_docker_images:
	@kind load docker-image $(DBPROXY_IMAGE) --name dbproxy-example
	@kind load docker-image $(TEST_SERVER_IMAGE) --name dbproxy-example
	@kind load docker-image $(MYSQL_IMAGE) --name dbproxy-example

.PHONY: k8s_up
k8s_up:
	@make pull_docker_images
	@make delete_cluster
	@make create_cluster
	@make load_docker_images

.PHONY: k8s_down
k8s_down:
	@make delete_cluster

.PHONY: sidecar_up
sidecar_up:
	@kubectl apply -f ./sidecar/dbproxy.yaml

.PHONY: sidecar_down
sidecar_down:
	@kubectl delete -f ./sidecar/dbproxy.yaml
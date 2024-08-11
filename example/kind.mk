# 定义变量
DBPROXY_IMAGE := flycash/dbproxy:dbproxy-v0.5
TEST_SERVER_IMAGE := flycash/dbproxy:dbproxy-app-v0.1
MYSQL_IMAGE := mysql:8.0.29

IMAGES := $(DBPROXY_IMAGE) $(TEST_SERVER_IMAGE) $(MYSQL_IMAGE)

ROOTDIR := $(abspath $(dir $(lastword $(MAKEFILE_LIST))))

.PHONY: print
print:
	@echo  $(lastword $(MAKEFILE_LIST))
	@echo  $(dir $(lastword $(MAKEFILE_LIST)))

.PHONY: pull_docker_images
pull_docker_images:
	@for img in $(IMAGES); do \
		docker pull $$img; \
		echo "\n"; \
	done

# 定义集群名称
CLUSTER_NAME := dbproxy-example

.PHONY: create_cluster
create_cluster:
	@kind create cluster --name $(CLUSTER_NAME) --config $(ROOTDIR)/kind-config.yaml

.PHONY: delete_cluster
delete_cluster:
	@kind delete cluster --name $(CLUSTER_NAME)

# 将镜像导入集群
.PHONY: load_images_into_cluster
load_images_into_cluster:
	@kind load docker-image $(DBPROXY_IMAGE) --name dbproxy-example
	@kind load docker-image $(TEST_SERVER_IMAGE) --name dbproxy-example
	@kind load docker-image $(MYSQL_IMAGE) --name dbproxy-example

.PHONY: k8s_up
k8s_up: pull_docker_images k8s_down create_cluster load_images_into_cluster

.PHONY: k8s_down
k8s_down:
	@make delete_cluster
# 定义变量
DBPROXY_IMAGE := flycash/dbproxy:dbproxy-v0.5
APP_IMAGE := flycash/dbproxy:dbproxy-app-v0.1
MYSQL_IMAGE := mysql:8.0.29

IMAGES := $(DBPROXY_IMAGE) $(APP_IMAGE) $(MYSQL_IMAGE)

ROOTDIR := $(abspath $(dir $(lastword $(MAKEFILE_LIST))))

# 如果你单独构建了镜像,你需要执行make k8s_up将kind节点上的老版本镜像删除
# --progress plain
.PHONY: build_app_image
build_app_image:
	@docker build --quiet -t $(APP_IMAGE) -f ./Dockerfile .
	
.PHONY: pull_docker_images
pull_docker_images: build_app_image
	@for img in $(IMAGES); do \
		docker pull $$img; \
		echo "\n"; \
	done

# 定义集群名称
CLUSTER_NAME := dbproxy-example

.PHONY: create_cluster
create_cluster:
	@if ! kind get clusters 2>/dev/null | grep -q "^$(CLUSTER_NAME)$$"; then \
		kind create cluster --name $(CLUSTER_NAME) --config $(ROOTDIR)/kind-config.yaml; \
	fi

.PHONY: delete_cluster
delete_cluster:
	@if kind get clusters 2>/dev/null | grep -q "^$(CLUSTER_NAME)$$"; then \
		kind delete cluster --name $(CLUSTER_NAME); \
	fi

.PHONY: delete_images_from_cluster
delete_images_from_cluster: create_cluster
	@for node in $$(kind get nodes --name $(CLUSTER_NAME)); do \
		images_list=$$(docker exec -i $${node} crictl images | tail -n +2 | awk '{print $$1":"$$2}'); \
		IFS=$$'\n'; \
		for image in $$images_list; do \
			image_name=$$(echo $$image | cut -d: -f1); \
			image_tag=$$(echo $$image | cut -d: -f2); \
			combined_name_tag=$$image_name:$$image_tag; \
			for img in $(IMAGES); do \
				case "$$combined_name_tag" in *$$img*) \
						docker exec -i $${node} crictl rmi "$$combined_name_tag"; \
						;; \
				esac; \
			done; \
		done; \
	done

# 将镜像导入集群
.PHONY: load_images_into_cluster
load_images_into_cluster: delete_images_from_cluster
	@for img in $(IMAGES); do \
		kind load docker-image $$img --name $(CLUSTER_NAME); \
	done

.PHONY: k8s_up
k8s_up: k8s_down load_images_into_cluster
	@echo "\nK8s集群部署成功\n"

.PHONY: k8s_down
k8s_down: delete_cluster
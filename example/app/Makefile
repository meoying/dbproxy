
# 引用常用kind命令
include ../kind.mk

NAMESPACE:=sidecar

# 创建命名空间
.PHONY: create_ns
create_ns:
	@kubectl get namespace $(NAMESPACE) &>/dev/null || kubectl create namespace $(NAMESPACE)

.PHONY: delete_ns
delete_ns:
	@if kubectl get namespace $(NAMESPACE) &>/dev/null; then \
		kubectl delete namespace $(NAMESPACE); \
	fi

# 创建 configmap 用于保存整个应用的各种配置文件
.PHONY: create_cm
create_cm: create_ns
	@kubectl get configmap app-configmap --namespace=$(NAMESPACE) &>/dev/null || kubectl create configmap app-configmap \
	--from-file=config/ --from-file=scripts//mysql/init.sql --namespace=$(NAMESPACE)

.PHONY: delete_cm
delete_cm:
	@if kubectl get configmap app-configmap --namespace=$(NAMESPACE) &>/dev/null; then \
		kubectl delete configmap app-configmap --namespace=$(NAMESPACE); \
	fi
# 演示 sidecar 部署模式时,用于部署单个组件
.PHONY: deploy.sidecar.%
deploy.sidecar.%:
	@kubectl apply -f deployments/sidecar/$*.yaml --namespace=$(NAMESPACE)

# 演示 sidecar 部署模式时,用于删除单个组件
.PHONY: undeploy.sidecar.%
undeploy.sidecar.%:
	@kubectl delete -f deployments/sidecar/$*.yaml --namespace=$(NAMESPACE)

# 演示 sidecar 部署模式时,一键部署整个应用
.PHONY: deploy_sidecar
deploy_sidecar: create_cm deploy.sidecar.mysql deploy.sidecar.app

# 演示 sidecar 部署模式时,一键删除整个应用
.PHONY: undeploy_sidecar
undeploy_sidecar: undeploy.sidecar.app undeploy.sidecar.mysql delete_cm delete_ns


# 演示 sidecar container 部署模式时,用于部署单个组件
.PHONY: deploy.sidecarcontainer.%
deploy.sidecarcontainer.%:
	@kubectl apply -f deployments/sidecar_container/$*.yaml --namespace=$(NAMESPACE)

# 演示 sidecar container 部署模式时,用于删除单个组件
.PHONY: undeploy.sidecarcontainer.%
undeploy.sidecarcontainer.%:
	@kubectl delete -f deployments/sidecar_container/$*.yaml --namespace=$(NAMESPACE)

# 演示 sidecar container 部署模式时,一键部署整个应用
.PHONY: deploy_sidecarcontainer
deploy_sidecarcontainer: create_cm deploy.sidecarcontainer.mysql deploy.sidecarcontainer.app

# 演示 sidecar container 部署模式时,一键删除整个应用
.PHONY: undeploy_sidecarcontainer
undeploy_sidecarcontainer: undeploy.sidecarcontainer.app undeploy.sidecarcontainer.mysql delete_cm delete_ns
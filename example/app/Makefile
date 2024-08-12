
# 引用常用kind命令
include ../kind.mk

NAMESPACE:=sidecar

# 创建命名空间
.PHONY: create_ns
create_ns:
	@kubectl get namespace $(NAMESPACE) &>/dev/null || kubectl create namespace $(NAMESPACE)

.PHONY: delete_ns
delete_ns:
	@kubectl delete namespace $(NAMESPACE)

# 创建configmap保存各种配置文件
.PHONY: create_cm
create_cm: create_ns
	@kubectl create configmap app-configmap \
	--from-file=config/ --from-file=scripts//mysql/init.sql --namespace=$(NAMESPACE)

.PHONY: delete_cm
delete_cm:
	@kubectl delete configmap app-configmap --namespace=$(NAMESPACE)

# 使用sidecar方式部署
.PHONY: deploy.sidecar.%
deploy.sidecar.%:
	@kubectl apply -f deployments/sidecar/$*.yaml --namespace=$(NAMESPACE)

.PHONY: undeploy.sidecar.%
undeploy.sidecar.%:
	@kubectl delete -f deployments/sidecar/$*.yaml --namespace=$(NAMESPACE)

.PHONY: deploy_sidecar
deploy_sidecar: create_cm deploy.sidecar.mysql deploy.sidecar.app

.PHONY: undeploy_sidecar
undeploy_sidecar: undeploy.sidecar.app undeploy.sidecar.mysql delete_cm delete_ns

# 使用sidecar container 方式部署
.PHONY: deploy.sidecarcontainer.%
deploy.sidecarcontainer.%:
	@kubectl apply -f deployments/sidecar_container/$*.yaml --namespace=$(NAMESPACE)

.PHONY: undeploy.sidecarcontainer.%
undeploy.sidecarcontainer.%:
	@kubectl delete -f deployments/sidecar_container/$*.yaml --namespace=$(NAMESPACE)

.PHONY: deploy_sidecarcontainer
deploy_sidecarcontainer: create_cm deploy.sidecarcontainer.mysql deploy.sidecarcontainer.app

.PHONY: undeploy_sidecarcontainer
undeploy_sidecarcontainer: undeploy.sidecarcontainer.app undeploy.sidecarcontainer.mysql delete_cm delete_ns
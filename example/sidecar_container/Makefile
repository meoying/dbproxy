apply-sharding:
	kubectl apply -f forward_config.yaml

apply-config:
	kubectl apply -f config.yaml
apply-deployment:
	kubectl apply -f dbproxy.yaml

deploy: apply-sharding apply-config create-configmap apply-deployment
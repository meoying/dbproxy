create-configmap:
	kubectl create configmap mysql-init-script-configmap --from-file=init.sql
create-mysql:
	kubectl apply -f config.yaml
mysql: create-configmap create-mysql
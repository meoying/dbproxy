.PHONY: setup
setup:
	@sh ./.script/setup.sh

.PHONY: tidy
tidy:
	@go mod tidy -v

.PHONY: fmt
fmt:
	@sh ./.script/fmt.sh

.PHONY: lint
lint:
	@golangci-lint run -c ./.script/.golangci.yml

.PHONY: check
check:
	@$(MAKE) --no-print-directory tidy
	@$(MAKE) --no-print-directory fmt
	@$(MAKE) --no-print-directory lint

# 单元测试
.PHONY: ut
ut:
	@echo "运行单元测试中......"
	@go test -race -failfast -count=1 ./...

# e2e 测试
.PHONY: e2e
e2e:
	@echo "运行集成测试中......"
	@sh ./.script/integrate_test.sh

.PHONY: e2e_up
e2e_up:
	docker compose -p dbproxy -f .script/integration_test_compose.yml up -d

.PHONY: e2e_down
e2e_down:
	docker compose -p dbproxy -f .script/integration_test_compose.yml down -v

.PHONY: test
test:
	@make ut
	@make e2e

# 定义镜像变量
IMAGE_VERSION ?= v0.5
IMAGE_NAME = flycash/dbproxy:dbproxy-$(IMAGE_VERSION)

.PHONY: build_docker_image
build_docker_image:
	@docker build --progress plain -t $(IMAGE_NAME) -f ./Dockerfile .
	@make update_compose_file

.PHONY: update_compose_file
update_compose_file:
	@sed -i.bak -e "/dbproxy-forward:/,/image:/s|image:.*|image: $(IMAGE_NAME)|" \
    	          -e "/dbproxy-sharding:/,/image:/s|image:.*|image: $(IMAGE_NAME)|" \
    	          ./.script/integration_test_compose.yml
	@rm ./.script/integration_test_compose.yml.bak # 删除备份文件

.PHONY: push_docker_image
push_docker_image:
	@echo "上传镜像 $(IMAGE_NAME) 中.....需要先执行docker login"
	@docker push $(IMAGE_NAME)
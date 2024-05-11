# 单元测试
.PHONY: ut
ut:
	@go test -race ./...

.PHONY: setup
setup:
	@sh ./.script/setup.sh

.PHONY: lint
lint:
	golangci-lint run

.PHONY: fmt
fmt:
	@sh ./.script/fmt.sh
# 使用一个带有cgo的golang构建镜像，因为编译插件的时候需要cgo
FROM  bitnami/golang:1.22 AS build

MAINTAINER flycash

# 设置工作目录
WORKDIR /app

# 将本地文件复制到工作目录
COPY . .

WORKDIR /app/cmd/docker_image
ENV GOPROXY=https://goproxy.cn

# 编译 Go 应用程序
RUN go mod tidy
RUN CGO_ENABLED=1 go generate ./...
RUN go build -o dbproxy .


FROM  debian:trixie-slim

# 设置工作目录
WORKDIR /app/dbproxy

RUN mkdir log

# 拷贝dbproxy二进制文件
COPY --from=build /app/cmd/docker_image/dbproxy .
# 拷贝dbproxy主配置文件
COPY --from=build /app/cmd/docker_image/config/config.yaml config.yaml
# 拷贝dbproxy插件及插件配置文件
COPY --from=build /app/cmd/docker_image/plugins ./plugins

# 运行应用程序
CMD ["./dbproxy"]



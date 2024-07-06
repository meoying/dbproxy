# 使用一个带有cgo的golang构建镜像，因为编译插件的时候需要cgo
FROM  bitnami/golang:1.22 AS build

# 设置工作目录
WORKDIR /app

# 将本地文件复制到工作目录
COPY . .

WORKDIR /app/cmd/proxy
ENV GOPROXY=https://goproxy.cn

# 编译 Go 应用程序
RUN CGO_ENABLED=1 go generate ./...
RUN   go build -o proxy .


FROM  debian:trixie-slim

# 设置工作目录
WORKDIR /root/

# 从之前的构建阶段复制二进制文件到当前镜像
COPY --from=build /app/cmd/proxy/proxy /root

COPY --from=build /app/cmd/proxy/plugin /root/plugin
COPY --from=build /app/cmd/proxy/config /root/config


# 运行应用程序
CMD ["./proxy"]



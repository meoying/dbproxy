# 使用官方的 golang 基础镜像
FROM golang:1.22.0-alpine AS build

# 设置工作目录
WORKDIR /app

# 将本地文件复制到工作目录
COPY . .

WORKDIR /app/cmd/proxy
# 编译 Go 应用程序
RUN go build -o proxy .

# 使用轻量的 alpine 作为基础镜像
FROM alpine:latest

# 设置工作目录
WORKDIR /root/

# 从之前的构建阶段复制二进制文件到当前镜像
COPY --from=build /app/cmd/proxy/proxy .

COPY --from=build /app/cmd/proxy/plugin /root/plugin
COPY --from=build /app/cmd/proxy/config /root/config


#CMD ["sh", "-c", "cd /root/sharding && ls -l"]
CMD ["sh", "-c", "ls -l"]
# 运行应用程序
#CMD ["./proxy"]

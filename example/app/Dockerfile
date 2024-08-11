# 使用官方的 Golang 基础镜像作为构建环境
FROM golang:1.22.0-alpine AS build

MAINTAINER flycash

# 设置工作目录
WORKDIR /app

# 将本地文件复制到工作目录
COPY . .

RUN ls -al .

# 如果有需要，可以设置代理等环境变量
ENV GOPROXY=https://goproxy.cn

# 编译 Go 应用程序
RUN go mod tidy
RUN go build -o app .

RUN ls -al .

# 使用轻量的 Alpine 作为基础镜像来运行应用程序
FROM alpine:3.20

# 可选：如果应用程序需要依赖运行时库，可以在这里安装
# RUN apk --no-cache add ca-certificates

# 设置工作目录
WORKDIR /app

# 从构建阶段复制二进制文件到当前镜像
COPY --from=build /app/app /app/app

RUN ls -al .

# 运行应用程序
#CMD ["ls","-l"]
CMD ["/app/app"]

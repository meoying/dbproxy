# Build the webhook binary
FROM golang:1.22.0-alpine as builder


WORKDIR /root


# Build
ENV GOPROXY="https://goproxy.cn"
COPY . .

RUN go build -o webhook .

FROM alpine:latest
COPY --from=builder /root/webhook /root/webhook
ENTRYPOINT ["/root/webhook"]
#!/usr/bin/env bash

set -e
docker compose -p dbproxy -f .script/integration_test_compose.yml down -v
docker compose -p dbproxy -f .script/integration_test_compose.yml up -d
#sudo echo "127.0.0.1 slave.a.com" >> /etc/hosts
go test -timeout=30m -race ./... -tags=e2e -count=1
docker compose -p dbproxy -f .script/integration_test_compose.yml down -v

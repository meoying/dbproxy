package main

import (
	"github.com/meoying/dbproxy/internal/protocol/mysql/plugin/sharding"
)


//go:generate go build  --buildmode=plugin   -o sharding.so ./sharding.go


var Plugin sharding.Plugin

package main

import (
	"github.com/meoying/dbproxy/internal/protocol/mysql/plugin/log"
)

//go:generate go build  --buildmode=plugin   -o log.so ./log.go

var Plugin log.Plugin

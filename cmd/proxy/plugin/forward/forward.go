package main

import (
	"github.com/meoying/dbproxy/internal/protocol/mysql/plugin/forward"
)

//go:generate go build  --buildmode=plugin   -o forward.so ./forward.go

var Plugin forward.Plugin
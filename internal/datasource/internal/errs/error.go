package errs

import (
	"fmt"
	"github.com/pkg/errors"
)

func NewErrNotCompleteFinder(name string) error {
	return fmt.Errorf("eorm: %s 未实现 Finder 接口", name)
}

func NewErrNotFoundTargetDataSource(name string) error {
	return fmt.Errorf("eorm: 未发现目标 data dource %s", name)
}


var ErrSlaveNotFound = errors.New(" slave不存在")

func NewInvalidDSNError(dsn string) error {
	return fmt.Errorf("不正确的 DSN %s", dsn)
}
func NewErrNotFoundTargetDB(name string) error {
	return fmt.Errorf(" 未发现目标 DB %s", name)
}

func NewErrDBNotEqual(oldDB, tgtDB string) error {
	return fmt.Errorf("禁止跨库操作： %s 不等于 %s ", oldDB, tgtDB)
}
var ErrUnsupportedDistributedTransaction = errors.New(" 不支持的分布式事务类型")

func NewFailedToGetSlavesFromDNS(err error) error {
	return fmt.Errorf("从DNS中解析从库失败 %w", err)
}
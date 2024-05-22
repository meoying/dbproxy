package errs

import "fmt"

func NewErrNotCompleteFinder(name string) error {
	return fmt.Errorf("eorm: %s 未实现 Finder 接口", name)
}

func NewErrNotFoundTargetDataSource(name string) error {
	return fmt.Errorf("eorm: 未发现目标 data dource %s", name)
}

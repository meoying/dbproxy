package errs

import "errors"

var (
	ErrConfigSyntaxInvalid           = errors.New("配置文件语法错误")
	ErrVariableNameNotFound          = errors.New("变量名称找不到")
	ErrVariableTypeInvalid           = errors.New("变量类型非法")
	ErrUnmarshalVariableFailed       = errors.New("反序列化变量失败")
	ErrVariableTypeNotEvaluable      = errors.New("变量类型不可求值")
	ErrReferencedVariableTypeInvalid = errors.New("引用的变量类型非法")
	ErrReferencePathInvalid          = errors.New("引用路径非法")
)

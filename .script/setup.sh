SOURCE_COMMIT=.github/pre-commit
TARGET_COMMIT=.git/hooks/pre-commit
SOURCE_PUSH=.github/pre-push
TARGET_PUSH=.git/hooks/pre-push

echo "设置 git pre-commit hooks..."
cp -f $SOURCE_COMMIT $TARGET_COMMIT

echo "设置 git pre-push hooks..."
cp -f $SOURCE_PUSH $TARGET_PUSH

# add permission to TARGET_PUSH and TARGET_COMMIT file.
test -x $TARGET_PUSH || chmod +x $TARGET_PUSH
test -x $TARGET_COMMIT || chmod +x $TARGET_COMMIT

echo "安装 golangci-lint..."
go install github.com/golangci/golangci-lint/cmd/golangci-lint@v1.52.2

echo "安装 goimports..."
go install golang.org/x/tools/cmd/goimports@latest

echo "下载子模块 antlr......如果失败请现在github上配置SSH Authentication Key"
git submodule update --init --recursive
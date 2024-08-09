# Builder 包使用说明
1. 使用`b := NewXXXPacket`方法来创建一个包构建器,构造函数参数通常是与conn、server状态相关的内容
2. `b.XXXX = YYYY` 来对包字段进行赋值
3. `b.Build()`得到包的二进制字节数组
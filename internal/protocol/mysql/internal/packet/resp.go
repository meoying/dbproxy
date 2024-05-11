package packet

import (
	"encoding/binary"
)

// 构造返回给客户端响应的 packet

// BuildErrRespPacket 构造一个错误响应给客户端
func BuildErrRespPacket(err ErrorResp) []byte {
	// 头部四个字节保留
	res := make([]byte, 4, 13+len(err.msg))
	// 固定 0xFF 代表错误
	res = append(res, 0xFF)
	// 错误码
	res = binary.LittleEndian.AppendUint16(res, err.code)
	// 我们是必然支持 CLIENT_PROTOCOL_41，所以要加 state 相关字段
	// 固定的 # 作为分隔符
	res = append(res, '#')
	res = append(res, err.state...)

	// 最后是人可读的错误信息
	res = append(res, err.msg...)
	return res
}

func BuildOKResp(status SeverStatus) []byte {
	// 头部的四个字节保留，不需要填充
	res := make([]byte, 4, 11)
	// 0 代表OK响应
	res = append(res, 0)
	// 0 影响行数
	res = append(res, 0)
	// 0 last_insert_id
	res = append(res, 0)
	// 服务器状态
	res = binary.LittleEndian.AppendUint16(res, status.AsUint16())
	// warning number 0 0
	res = append(res, 0, 0)
	return res
}

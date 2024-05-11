package cmd

type Cmd byte

func (c Cmd) Byte() byte {
	return byte(c)
}

const (
	CmdQuit Cmd = iota + 1
	CmdInitDB
	CmdQuery
	CmdFieldList
	CmdCreateDB
	CmdDropDB
	CmdRefresh
	CmdShutdown
	CmdStatistics
	CmdProcessInfo
	CmdConnect
	CmdProcessKill
	CmdDebug
	CmdPing
	CmdTime
	CmdDelayedInsert
	CmdChangeUser
	CmdBinlogDump
	CmdTableDump
	CmdConnectOut
	CmdRegisterSlave
	CmdStmtPrepare
	CmdStmtExecute
	CmdStmtSendLongData
	CmdStmtClose
	CmdStmtReset
	CmdSetOption
	CmdStmtFetch
)

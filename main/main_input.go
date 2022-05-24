package main

import (
	"github.com/go-mysql-org/go-mysql/canal"
)

type MainInput struct {
	SourceCfg      *canal.Config  // 源mysql配置
	BinlogReciever BinlogReciever // binlog接受者配置。目前通过http发送远端
	SyncFrom       BinlogSyncInfo // 从什么gtid或者位点开始同步binlog
	EventSource    string         // cloud event 事件源
	EventType      string         // cloud event 事件类型
}

// 通过位点或者gtid同步
type BinlogSyncInfo struct {
	BinlogFileName string
	Offset         uint32
	// or
	Gtid string
}

func (r *BinlogSyncInfo) IsFromOffset() bool {
	if len(r.BinlogFileName) > 0 && r.Offset > 0 {
		return true
	}
	return false
}

func (r *BinlogSyncInfo) IsFromGtid() bool {
	if len(r.Gtid) > 0 {
		return true
	}
	return false
}

// 接受binlog的接口信息
type BinlogReciever struct {
	Url string // binlog发送的接口
}

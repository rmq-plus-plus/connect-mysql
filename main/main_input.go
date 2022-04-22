package main

import (
	"github.com/go-mysql-org/go-mysql/replication"
)

type MainInput struct {
	MysqlConfig    MockBinlogSyncerConfig // 源端mysql配置
	BinlogReciever BinlogReciever
	SyncInfo       BinlogSyncInfo
	EventSource string        // 事件源
	EventType   string        // 事件类型
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
	Url    string // binlog发送的接口
}

// 配置 参考 replication.BinlogSyncerConfig
type MockBinlogSyncerConfig struct {
	ServerID uint32

	Flavor string

	Host string

	Port uint16

	User string

	Password string

	Charset string
}

func (r *MockBinlogSyncerConfig) To() replication.BinlogSyncerConfig {
	var config = replication.BinlogSyncerConfig{
		ServerID: r.ServerID,
		Flavor:   r.Flavor,
		Host:     r.Host,
		Port:     r.Port,
		User:     r.User,
		Password: r.Password,
		Charset:  r.Charset,
	}
	return config
}

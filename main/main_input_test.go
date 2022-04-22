package main

import (
	"encoding/json"
	"fmt"
	"testing"
)

func TestBuildMainInput(t *testing.T) {
	var input = &MainInput{
		MysqlConfig: MockBinlogSyncerConfig{
			ServerID: 1002053,
			Flavor:   "mysql",
			Host:     "127.0.0.1",
			Port:     3306,
			User:     "cdc",
			Password: "cdc666",
		},
		BinlogReciever: BinlogReciever{
			Url: "http://xxx.xxx.xxx:8280/accept_binlog",
		},
		SyncInfo: BinlogSyncInfo{
			Gtid: "4d0469b8-f355-11eb-83cd-525400a567c5:1-32222801",
			//BinlogFileName: "mysql-bin.000021",
			//Offset:         4,
		},
		EventSource: "source xxx",
		EventType:   "type xxx",
	}

	if bytes, err := json.MarshalIndent(input, "", " "); err == nil {
		fmt.Println(string(bytes))
	}

}

package main

import (
	"encoding/json"
	"fmt"
	"github.com/go-mysql-org/go-mysql/canal"
	"testing"
)

func TestBuildMainInput(t *testing.T) {
	var input = &MainInput{
		SourceCfg: &canal.Config{
			ServerID: 1002053,
			Flavor:   "mysql",
			Addr:     "127.0.0.1:3306",
			User:     "cdc",
			Password: "cdc666",
		},
		BinlogReciever: BinlogReciever{
			Url: "http://xxx.xxx.xxx:8280/accept_binlog",
		},
		SyncFrom: BinlogSyncInfo{
			Gtid: "4d0469b8-f355-11eb-83cd-525400a567c5:1-32222801",
			//BinlogFileName: "mysql-bin.000021",
			//Offset:         4,
		},
		EventSource: "sourcexxx",
		EventType:   "typexxx",
	}

	if bytes, err := json.MarshalIndent(input, "", " "); err == nil {
		fmt.Println(string(bytes))
	}

}

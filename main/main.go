package main

import (
	"01-connect-mysql/main/river"
	"encoding/json"
	"fmt"
	"github.com/go-mysql-org/go-mysql/mysql"
	"os"
)

func main() {
	var inputJsonString = ""
	if len(os.Args) > 1 {
		inputJsonString = os.Args[1]
	} else {
		fmt.Println("输入参数错误")
		return
	}
	var input = &MainInput{}
	err := json.Unmarshal([]byte(inputJsonString), input)
	if err != nil {
		fmt.Println("输入参数解析失败" + err.Error() + "\n" + inputJsonString)
	}

	initInput(input)
}

func initInput(input *MainInput) {
	var binlogDumper = river.NewBinlogDumper(
		input.MysqlConfig.To(),
		river.NewBinlogHttpWirter(input.BinlogReciever.Url, input.EventSource, input.EventType),
	)
	if input.SyncInfo.IsFromGtid() {
		binlogDumper.ReceiveBinlogByGtid(mysql.MySQLFlavor, input.SyncInfo.Gtid)
		return
	}
	if input.SyncInfo.IsFromOffset() {
		binlogDumper.ReceiveBinlogByPostion(input.SyncInfo.BinlogFileName, input.SyncInfo.Offset)
		return
	}
	fmt.Println("gtid和位点信息都没有传入， 至少传一个")
}

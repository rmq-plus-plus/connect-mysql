package main

import (
	"01-connect-mysql/main/river"
	"01-connect-mysql/main/utils"
	"encoding/json"
	"fmt"
	"os"
)

func main() {
	var inputFile = ""
	if len(os.Args) > 1 {
		inputFile = os.Args[1]
	} else {
		fmt.Println("请指定输入参数文件路径")
		return
	}
	inputString, err1 := utils.ReadAllFile(inputFile)
	if err1 != nil {
		fmt.Println("输入参数文件读取失败" + err1.Error())
		return
	}
	var input = &river.MainInput{}
	err2 := json.Unmarshal([]byte(inputString), input)
	if err2 != nil {
		fmt.Println("输入参数解析失败" + err2.Error() + "\n" + inputString)
	}

	initInput(input)
}

func initInput(input *river.MainInput) {
	writer := river.NewBinlogHttpWirter(input.BinlogReciever.Url, input.EventSource, input.EventType)
	var binlogDumper = river.NewBinlogDumper(
		input.SourceCfg,
		river.NewHandler(writer),
	)
	if input.SyncFrom.IsFromGtid() {
		err := binlogDumper.StartFromGTID(input.SyncFrom.Gtid)
		if err != nil {
			fmt.Println("StartFromGTID失败", err)
			return
		}
		return
	}

	fmt.Println("gtid和位点信息都没有传入， 至少传一个")
}

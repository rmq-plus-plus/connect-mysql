package river

import (
	"fmt"
	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/mysql"
	_ "github.com/gogf/gf/v2/frame/g"
	_ "github.com/gogf/gf/v2/net/gclient"
	"testing"
)

func TestDumpSomeTables(t *testing.T) {
	eventSource := "sourceXXXX"
	eventType := "typeXXX"
	cfg := &canal.Config{
		ServerID: 1002053,
		Flavor:   mysql.MySQLFlavor,
		Addr:     "127.0.0.1:3306",
		User:     "root",
		Password: "xxxxxxx",
		Dump: canal.DumpConfig{
			ExecutionPath: "/usr/bin/mysqldump",
			DiscardErr:    false,
		},
	}
	writer := NewBinlogHttpWirter("http://127.0.0.1:8280/accept_binlog", eventSource, eventType)
	binlogDumper := NewBinlogDumper(
		cfg,
		NewDataHandler(writer),
	)

	binlogDumper.AddDumpIgnoreSystemTables()
	err := binlogDumper.Dump()
	if err != nil {
		fmt.Println("启动dump失败", err)
		return
	}

	gtidString := "4d0469b8-f355-11eb-83cd-525400a567c5:1-32222868"
	err = binlogDumper.StartFromGTID(gtidString)
	if err != nil {
		fmt.Println("启动binlog同步失败", err)
		return
	}
}

package river

import (
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
		Addr:     "127.0.0.1:10005",
		User:     "xxxxx",
		Password: "xxxxx",
	}

	writer := NewBinlogHttpWirter("http://127.0.0.1:8280/accept_binlog", eventSource, eventType)
	binlogDumper := NewBinlogDumper(
		cfg,
		NewHandler(writer),
	)
	gtidString := "4d0469b8-f355-11eb-83cd-525400a567c5:1-32222868"

	_ = binlogDumper.StartFromGTID(gtidString)
}

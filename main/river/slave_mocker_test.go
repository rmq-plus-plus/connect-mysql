package river

import (
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"testing"
	"time"

	_ "github.com/gogf/gf/v2/frame/g"
	_ "github.com/gogf/gf/v2/net/gclient"
)

func TestReceiveBinlogByGtid(t *testing.T) {
	cfg := replication.BinlogSyncerConfig{
		ServerID: 1002053,
		Flavor:   "mysql",
		Host:     "xxxxx",
		Port:     10005,
		User:     "root",
		Password: "xxxxx",
	}

	binlogDumper := NewBinlogDumper(cfg, NewBinlogHttpWirter("http://xxxxx:8280/accept_binlog", "sourceXXXX", "typeXXX"))
	binlogDumper.ReceiveBinlogByGtid(mysql.MySQLFlavor, "4d0469b8-f355-11eb-83cd-525400a567c5:1-32222816")

	time.Sleep(100000000)
}

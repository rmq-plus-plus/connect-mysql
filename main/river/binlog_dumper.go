package river

import (
	"fmt"
	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
)

type BinlogDumper struct {
	canal  *canal.Canal
	config replication.BinlogSyncerConfig // mysql master 配置
}

func NewBinlogDumper(config *canal.Config, handler canal.EventHandler) *BinlogDumper {
	binlogDumper := &BinlogDumper{}
	cccanal, err := canal.NewCanal(config)
	if err != nil {
		fmt.Println("初始化binlog dumper失败。配置错误" + err.Error())
		panic(err)
	}
	binlogDumper.canal = cccanal
	binlogDumper.canal.SetEventHandler(handler)

	return binlogDumper
}

func (t *BinlogDumper) Close() {
	t.canal.Close()
}

// 指定gtid开始同步binlog
func (t *BinlogDumper) StartFromGTID(gtidString string) error {
	set, eror := mysql.ParseGTIDSet(mysql.MySQLFlavor, gtidString)
	if eror != nil {
		fmt.Println("StartFromGTID失败，ParseGTIDSet失败")
		return eror
	}
	return t.canal.StartFromGTID(set)
}

// 全量导出：启动全量dump
func (t *BinlogDumper) StartDump() error {
	return t.canal.Dump()
}

// 全量导出：启动全量dump
func (t *BinlogDumper) WaitDumpDone() {
	data := t.canal.WaitDumpDone()
	for msg := range data {
		fmt.Println(msg)
	}
}

// 全量导出：添加需要导出的库表信息。 入参key=db， value=table
func (t *BinlogDumper) AddDumpDatabaseAndTables(dbAndTablesMap map[string][]string) {
	if len(dbAndTablesMap) > 0 {
		for db, tbls := range dbAndTablesMap {
			for index := range tbls {
				t.canal.AddDumpTables(db, tbls[index])
			}
		}
	}
}

// 全量导出：添加不需要导出的库表信息。 入参key=db， value=table
func (t *BinlogDumper) AddDumpIgnoreTables(dbAndTablesMap map[string][]string) {
	if len(dbAndTablesMap) > 0 {
		for db, tbls := range dbAndTablesMap {
			for index := range tbls {
				t.canal.AddDumpIgnoreTables(db, tbls[index])
			}
		}
	}
}

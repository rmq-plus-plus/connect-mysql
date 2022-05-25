package river

import (
	"fmt"
	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/client"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
)

type BinlogDumper struct {
	canal  *canal.Canal
	config replication.BinlogSyncerConfig // mysql master 配置
	cfg    *canal.Config
}

func NewBinlogDumper(config *canal.Config, handler canal.EventHandler) *BinlogDumper {
	binlogDumper := &BinlogDumper{}
	binlogDumper.cfg = config
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

// 不要导mysql系统表
func (t *BinlogDumper) AddDumpIgnoreSystemTables() {
	var dbs = []string{
		"performance_schema", "mysql", "sys", "information_schema",
	}
	conn, err := client.Connect(t.cfg.Addr, t.cfg.User, t.cfg.Password, "")
	if err != nil {
		fmt.Println("AddDumpIgnoreSystemTables打开连接失败，", err.Error())
		return
	}
	for _, db := range dbs {
		r, err := conn.Execute(fmt.Sprintf("show tables from %s", db))
		if err != nil {
			fmt.Println("AddDumpIgnoreSystemTables失败，", err.Error())
			break
		}
		defer r.Close()
		for _, row := range r.Values {
			for _, val := range row {
				t.canal.AddDumpIgnoreTables(db, string(val.AsString()))
				fmt.Println(fmt.Printf("添加忽略系统表: %s.%s", db, string(val.AsString())))
			}
		}
	}

}

func (t *BinlogDumper) Dump() error {
	return t.canal.Dump()
}

func (t *BinlogDumper) DumpDataAndBinlog(gtidString string) error {
	err := t.Dump()
	if err != nil {
		fmt.Println("启动dump失败", err)
		return err
	}

	err = t.StartFromGTID(gtidString)
	if err != nil {
		fmt.Println("启动binlog同步失败", err)
		return err
	}
	return nil
}

package river

import (
	"01-connect-mysql/main/utils"
	"fmt"
	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
)

type MyBinlogEvent struct {
	DatabaseName string   //
	TableName    string   //
	Data         []string // 一次一行， 这里包含每个字段的值
	EventType    string   //  key枚举：RotateEvent，QueryEvent，RowsEvent
}

type DataHandler struct {
	Sender *BinlogHttpWirter
}

func NewDataHandler(sender *BinlogHttpWirter) *DataHandler {
	return &DataHandler{
		Sender: sender,
	}
}

// binlog文件变化
func (s *DataHandler) OnRotate(e *replication.RotateEvent) error {
	return nil
}

// 表结构表更
func (s *DataHandler) OnTableChanged(schema, table string) error {
	fmt.Println("OnTableChanged", schema, table)
	return nil
}

// ddl
func (s *DataHandler) OnDDL(nextPos mysql.Position, event *replication.QueryEvent) error {
	return nil
}

func (s *DataHandler) OnXID(nextPos mysql.Position) error {
	return nil
}

// dml
func (s *DataHandler) OnRow(rowsEvent *canal.RowsEvent) error {
	if len(rowsEvent.Rows) <= 0 {
		return nil
	}
	for rowIndex := range rowsEvent.Rows {
		dd := &MyBinlogEvent{
			DatabaseName: rowsEvent.Table.Schema,
			TableName:    rowsEvent.Table.Name,
			EventType:    "RowsEvent",
		}
		list := []string{}
		for colIndex := range rowsEvent.Rows[rowIndex] {
			list = append(list, utils.ToString(rowsEvent.Rows[rowIndex][colIndex]))
		}
		dd.Data = list
		//bytes, _ := json.Marshal(dd)
		err := s.Sender.Write(dd)
		fmt.Println(fmt.Sprintf("OnRow 发送结果:%v,error: %+v", err == nil, err))
	}
	return nil
}

// gtid变动
func (s *DataHandler) OnGTID(gtid mysql.GTIDSet) error {
	return nil
}

//
func (s *DataHandler) OnPosSynced(pos mysql.Position, set mysql.GTIDSet, force bool) error {
	return nil
}

func (s *DataHandler) String() string {
	return "DataHandler"
}

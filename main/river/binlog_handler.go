package river

import (
	"01-connect-mysql/main/utils"
	"encoding/json"
	"fmt"
	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"io"
)

type MyBinlogEvent struct {
	DatabaseName string   //
	TableName    string   //
	Data         []string // 一次一行， 这里包含每个字段的值
	EventType    string   //  key枚举：RotateEvent，QueryEvent，RowsEvent
}

type BinlogHandler struct {
	Sender io.Writer
}

func NewHandler(sender io.Writer) *BinlogHandler {
	return &BinlogHandler{
		Sender: sender,
	}
}

// binlog文件变化
func (s *BinlogHandler) OnRotate(e *replication.RotateEvent) error {
	return nil
}

// 表结构表更
func (s *BinlogHandler) OnTableChanged(schema, table string) error {
	fmt.Println("OnTableChanged", schema, table)
	return nil
}

// ddl
func (s *BinlogHandler) OnDDL(nextPos mysql.Position, event *replication.QueryEvent) error {
	return nil
}

func (s *BinlogHandler) OnXID(nextPos mysql.Position) error {
	return nil
}

// dml
func (s *BinlogHandler) OnRow(rowsEvent *canal.RowsEvent) error {
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
		bytes, _ := json.Marshal(dd)
		n, err := s.Sender.Write(bytes)
		fmt.Println(fmt.Sprintf("OnRow 发送结果:%v,error: %+v, 发送字节数: %d", err == nil, err, n))
	}
	return nil
}

// gtid变动
func (s *BinlogHandler) OnGTID(gtid mysql.GTIDSet) error {
	return nil
}

//
func (s *BinlogHandler) OnPosSynced(pos mysql.Position, set mysql.GTIDSet, force bool) error {
	return nil
}

func (s *BinlogHandler) String() string {
	return "BinlogHandler"
}

package river

import (
	"encoding/json"
	"fmt"
	"github.com/go-mysql-org/go-mysql/canal"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"io"
)

type MyBinlogEvent struct {
	DatabaseName string
	TableName    string //
	EventData    string // 不同EventType，对应不同的数据结构
	EventType    string //  key枚举：RotateEvent，QueryEvent，RowsEvent
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
	ee := &MyBinlogEvent{
		DatabaseName: string(event.Schema),
		EventType:    "QueryEvent",
	}
	ee1, _ := json.Marshal(event) // 出来的query是base64后的
	ee.EventData = string(ee1)
	bb, _ := json.Marshal(ee)

	n, err := s.Sender.Write(bb)
	fmt.Println(fmt.Sprintf("OnDDL发送结果: %v,error: %+v, 发送字节数: %d", err == nil, err, n))
	return nil
}

func (s *BinlogHandler) OnXID(nextPos mysql.Position) error {
	return nil
}

// dml
func (s *BinlogHandler) OnRow(rowsEvent *canal.RowsEvent) error {
	ee := &MyBinlogEvent{
		DatabaseName: rowsEvent.Table.Schema,
		TableName:    rowsEvent.Table.Name,
		EventType: "RowsEvent",
	}
	ee1, _ := json.Marshal(rowsEvent)
	ee.EventData = string(ee1)

	bb, _ := json.Marshal(ee)

	n, err := s.Sender.Write(bb)
	fmt.Println(fmt.Sprintf("OnRow 发送结果:%v,error: %+v, 发送字节数: %d", err == nil, err, n))
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

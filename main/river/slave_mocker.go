package river

import (
	"context"
	"fmt"
	"github.com/go-mysql-org/go-mysql/mysql"
	"github.com/go-mysql-org/go-mysql/replication"
	"io"
	"time"
)

type BinlogDumper struct {
	Sender io.Writer                      // 发送binlog配置
	config replication.BinlogSyncerConfig // mysql master 配置
}

func NewBinlogDumper(config replication.BinlogSyncerConfig, binlogSender *BinlogHttpWirter) *BinlogDumper {
	return &BinlogDumper{
		Sender: binlogSender,
		config: config,
	}
}

// receiveBinlogByPostion 从指定binlog文件和位点开始接受binlog
func (r *BinlogDumper) ReceiveBinlogByPostion(binlogFileName string, offset uint32) {
	pos := mysql.Position{
		Name: binlogFileName,
		Pos:  offset,
	}
	syncer := replication.NewBinlogSyncer(r.config)
	streamer, err := syncer.StartSync(pos)
	if err != nil {
		fmt.Println(fmt.Sprintf("ReceiveBinlogByPostion start sync error %+v", err))
		return
	}

	for {
		ev, err := streamer.GetEvent(context.Background())
		if ev.Header.EventType == replication.HEARTBEAT_EVENT || ev.Header.EventType == replication.PREVIOUS_GTIDS_EVENT {
			fmt.Println("HEARTBEAT_EVENT, 忽略")
			continue
		}
		if err != nil {
			fmt.Println(fmt.Sprintf("ReceiveBinlogByPostion get event error %+v", err))
			break
		}
		ev.Dump(r.Sender)
		time.Sleep(time.Second * 1)
	}
}

/*
receiveBinlogByGtid 从指定gtid接受binlog
masterConfig mysql master配置
gtidSet 开始接受binlog的gtid信息
*/
func (r *BinlogDumper) ReceiveBinlogByGtid(flavor string, gtidString string) {
	gtid, err := mysql.ParseGTIDSet(flavor, gtidString)
	if err != nil {
		fmt.Println(fmt.Sprintf("ReceiveBinlogByGtid ParseGTIDSet error %+v", err))
		return
	}
	syncer := replication.NewBinlogSyncer(r.config)
	streamer, err := syncer.StartSyncGTID(gtid)
	if err != nil {
		fmt.Println(fmt.Sprintf("ReceiveBinlogByGtid start sync error %+v", err))
		return
	}

	for {
		ev, err := streamer.GetEvent(context.Background())
		if ev.Header.EventType == replication.HEARTBEAT_EVENT || ev.Header.EventType == replication.PREVIOUS_GTIDS_EVENT {
			//fmt.Println("HEARTBEAT_EVENT, 忽略")
			continue
		}

		if err != nil {
			fmt.Println(fmt.Sprintf("ReceiveBinlogByGtid GetEvent error %+v", err))
			break
		}
		ev.Dump(r.Sender)
	}
}

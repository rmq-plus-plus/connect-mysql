package river

import (
	"context"
	"fmt"
	cloudevents "github.com/cloudevents/sdk-go/v2"
	"github.com/cloudevents/sdk-go/v2/client"
)

type BinlogHttpWirter struct {
	//io.Writer
	ctx context.Context

	client      client.Client // for cloud event http client
	url         string        // 接受数据的地址
	eventSource string        // 事件源
	eventType   string        // 事件类型

}

func NewBinlogHttpWirter(url string, eventSource string, eventType string) *BinlogHttpWirter {
	clientHTTP, _ := cloudevents.NewClientHTTP()
	ctx := cloudevents.ContextWithTarget(context.Background(), url)
	return &BinlogHttpWirter{
		ctx:         ctx,
		client:      clientHTTP,
		url:         url,
		eventSource: eventSource,
		eventType:   eventType,
	}
}

func (r *BinlogHttpWirter) Write(binlogBytes []byte) (bytesSent int, err error) {
	event := cloudevents.NewEvent()
	event.SetSource(r.eventSource)
	event.SetType(r.eventType)
	_ = event.SetData(cloudevents.ApplicationJSON, map[string]string{"data": string(binlogBytes)})

	err = r.client.Send(r.ctx, event)
	if cloudevents.IsUndelivered(err) {
		fmt.Println(fmt.Sprintf("write fail %+v", err))
		return 0, err
	}
	fmt.Println(fmt.Sprintf("write event done %+v", err))
	return len(binlogBytes), nil
}

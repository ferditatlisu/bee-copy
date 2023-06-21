package handler

import (
	"bee-copy/pkg/logger"
	"bee-copy/pkg/model"
	"bee-copy/pkg/service"
	"context"
	"fmt"
	"github.com/segmentio/kafka-go"
	"go.uber.org/zap"
	"time"
)

type listenerHandler struct {
	searchData  *model.CopyData
	from        service.CopyKafka
	to          service.CopyKafka
	partitionId int
	r           *service.RedisService
}

func NewListenerHandler(sd *model.CopyData, fromKafka service.CopyKafka, toKafka service.CopyKafka, partitionId int, r *service.RedisService) *listenerHandler {
	return &listenerHandler{searchData: sd, from: fromKafka, to: toKafka, partitionId: partitionId, r: r}
}

func (l *listenerHandler) Handle() {
	second := time.Duration(5)
	sd := l.searchData.FinishTime.UnixMilli()
	kc := l.from.CreateConsumer(l.searchData.FromTopic, l.partitionId)
	kp := l.to.CreateProducer(l.searchData.ToTopic)
	defer kc.Close()
	ctx, cancel := context.WithCancel(context.Background())
	t := time.AfterFunc(time.Second*second*2, func() {
		cancel()
	})
	for {
		m, err := kc.ReadMessage(ctx)
		if err != nil || m.Time.UnixMilli() > sd {
			break
		}
		err = l.publish(&m, kp, ctx)
		if err != nil {
			errMsg := fmt.Sprintf("publish error: %s", err.Error())
			l.r.AddLogError(errMsg)
			logger.Logger().Error("error occurred when message publish", zap.Error(err))
			return
		}

		t.Reset(time.Second * second)
	}
}

func (l *listenerHandler) publish(m *kafka.Message, kp *kafka.Writer, ctx context.Context) error {
	err := kp.WriteMessages(ctx, kafka.Message{
		Key:     m.Key,
		Value:   m.Value,
		Headers: m.Headers,
	})
	return err
}

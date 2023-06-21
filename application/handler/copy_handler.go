package handler

import (
	"bee-copy/pkg/config"
	"bee-copy/pkg/logger"
	"bee-copy/pkg/model"
	"bee-copy/pkg/model/status"
	"bee-copy/pkg/service"
	"fmt"
	"go.uber.org/zap"
	"net/http"
	"time"
)

type copyHandler struct {
	copyData  *model.CopyData
	fromKafka service.CopyKafka
	toKafka   service.CopyKafka
	redis     *service.RedisService
	conf      *config.ApplicationConfig
}

func NewCopyHandler(sd *model.CopyData,
	fromKafka service.CopyKafka,
	toKafka service.CopyKafka,
	redis *service.RedisService,
	c *config.ApplicationConfig) *copyHandler {
	return &copyHandler{copyData: sd, fromKafka: fromKafka, toKafka: toKafka, redis: redis, conf: c}
}

func (c *copyHandler) Handle() {
	s, err := c.redis.GetStateOfCopyEvent(c.copyData.Key)
	if err != nil {
		logger.Logger().Error("error occurred when HMGET from Redis", zap.Error(err))
		c.stopProcess(err)
		return
	}

	if s != status.REQUESTED {
		logger.Logger().Error("There is already process.")
		c.stopProcess(nil)
		return
	}

	err = c.redis.UpdateStateCopyEvent(c.copyData.Key, status.CREATED)
	if err != nil {
		c.stopProcess(err)
		return
	}

	err = c.hasTopic(c.copyData)
	if err != nil {
		logger.Logger().Error("Topic was not found", zap.Error(err))
		c.stopProcess(err)
		return
	}

	ch := NewConsumeHandler(c.fromKafka, c.toKafka, c.redis)
	err = ch.handle(c.copyData)
	if err != nil {
		c.stopProcess(err)
		return
	}

	err = c.redis.UpdateStateCopyEvent(c.copyData.Key, status.COMPLETED)
	c.stopProcess(err)
}

func (c *copyHandler) hasTopic(searchData *model.CopyData) error {
	_, err := c.fromKafka.GetPartitions(searchData.FromTopic)
	if err != nil {
		return err
	}

	_, err = c.toKafka.GetPartitions(searchData.ToTopic)
	if err != nil {
		return err
	}

	return nil
}

func (c *copyHandler) stopProcess(err error) {
	if err != nil {
		_ = c.redis.ErrorStateCopyEvent(c.copyData.Key, err.Error())
		logger.Logger().Error("process can not be complete", zap.Error(err))
	}

	c.deleteRequest(c.copyData.PodName)
}

func (c *copyHandler) deleteRequest(podName string) {
	fmt.Println("waiting before kill ...")
	select {
	case <-time.After(5 * time.Second):
		fmt.Println("ready to kill ...")
	}

	cli := &http.Client{}
	url := c.conf.Master.Url + "/master?key=" + podName
	req, err := http.NewRequest("DELETE", url, nil)
	if err != nil {
		logger.Logger().Error("Request couldn't created!", zap.Error(err))
		return
	}

	_, err = cli.Do(req)
	if err != nil {
		logger.Logger().Error("Request couldn't send to pod-master!", zap.Error(err))
		errMsg := fmt.Sprintf("Request couldn't send to pod-master!: %s", err.Error())
		c.redis.AddLogError(errMsg)
		return
	}
}

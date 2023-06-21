package handler

import (
	"bee-copy/pkg/logger"
	"bee-copy/pkg/model"
	"bee-copy/pkg/model/status"
	"bee-copy/pkg/service"
	"errors"
	"sync"
)

type consumeHandler struct {
	from  service.CopyKafka
	to    service.CopyKafka
	redis *service.RedisService
}

func NewConsumeHandler(fromKafka service.CopyKafka, toKafka service.CopyKafka, redis *service.RedisService) *consumeHandler {
	return &consumeHandler{from: fromKafka, to: toKafka, redis: redis}
}

func (c *consumeHandler) handle(copyData *model.CopyData) error {
	pIds := c.getPartitionIds(copyData)
	if len(pIds) == 0 {
		return errors.New("not available partitions for process")
	}

	var wg sync.WaitGroup
	wg.Add(len(pIds))
	logger.Logger().Info("Copy is in progress")
	err := c.redis.UpdateStateCopyEvent(copyData.Key, status.INPROGRESS)
	if err != nil {
		return err
	}

	c.redis.Increase()
	for i := range pIds {
		pId := pIds[i]
		go func(cd *model.CopyData, p int, r *service.RedisService) {
			h := NewListenerHandler(cd, c.from, c.to, p, r)
			h.Handle()
			wg.Done()
		}(copyData, pId, c.redis)
	}

	wg.Wait()
	return nil
}

func (c *consumeHandler) getPartitionIds(copyData *model.CopyData) []int {
	partitions, err := c.from.GetPartitions(copyData.FromTopic)
	if err != nil {
		return nil
	}

	pIds := make([]int, len(partitions))
	for i := range partitions {
		pIds[i] = partitions[i].ID
	}

	return pIds
}

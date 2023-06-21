package main

import (
	"bee-copy/application/handler"
	"bee-copy/pkg/config"
	"bee-copy/pkg/model"
	"bee-copy/pkg/service"
	"fmt"
	"os"
	"strconv"
	"time"
)

func main() {
	select {
	case <-time.After(15 * time.Second):
		fmt.Println("Waiting to start ...")
	}

	configInstance := config.CreateConfigInstance()
	appConf, _ := configInstance.GetConfig()
	fromKafka := getCopyKafka(appConf, os.Getenv("FROM_ID"))
	toKafka := getCopyKafka(appConf, os.Getenv("TO_ID"))
	redis := service.NewRedisService(appConf.Redis)
	copyData := model.NewCopyData(
		os.Getenv("POD_NAME"),
		os.Getenv("KEY"),
		os.Getenv("FROM_TOPIC"),
		os.Getenv("TO_TOPIC"),
	)

	ch := handler.NewCopyHandler(copyData, fromKafka, toKafka, redis, appConf)
	ch.Handle()
	fmt.Println("waiting to kill ...")
	if <-time.After(10 * time.Second); true {
		fmt.Println("still waiting ...")
	}
}

func getCopyKafka(config *config.ApplicationConfig, id string) service.CopyKafka {
	kafkaConfig := getKafkaConfig(config, id)
	if kafkaConfig.UserName == nil {
		return service.NewUnsecureCopyKafka(kafkaConfig.Host)
	}

	return service.NewSecureKafkaCopy(kafkaConfig)
}

func getKafkaConfig(config *config.ApplicationConfig, id string) *config.KafkaConfig {
	for i := range config.KafkaConfigs {
		kafkaConfig := config.KafkaConfigs[i]
		kafkaId := strconv.Itoa(kafkaConfig.Id)
		if id == kafkaId {
			return &kafkaConfig
		}
	}

	panic("Not available kafka config")
}

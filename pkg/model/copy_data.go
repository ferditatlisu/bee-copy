package model

import (
	"time"
)

type CopyData struct {
	PodName    string    `json:"podName"`
	Key        string    `json:"key"`
	FromTopic  string    `json:"fromTopic"`
	ToTopic    string    `json:"toTopic"`
	FinishTime time.Time `json:"finishTime"`
}

func NewCopyData(podName, key, fromTopic, toTopic string) *CopyData {
	return &CopyData{
		PodName:    podName,
		Key:        key,
		FromTopic:  fromTopic,
		ToTopic:    toTopic,
		FinishTime: time.Now().UTC(),
	}
}

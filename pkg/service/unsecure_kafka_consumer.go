package service

import (
	"context"
	"errors"
	"github.com/segmentio/kafka-go"
	"strings"
	"time"
)

type unsecureCopyKafka struct {
	s string
}

func NewUnsecureCopyKafka(servers string) CopyKafka {
	return &unsecureCopyKafka{servers}
}

func (k *unsecureCopyKafka) CreateConsumer(topic string, partition int) *kafka.Reader {
	readerConfig := kafka.ReaderConfig{
		Brokers:   strings.Split(k.s, ","),
		Partition: partition,
		Topic:     topic,
		MinBytes:  10e3, // 10KB
		MaxBytes:  10e6, // 10MB
		MaxWait:   time.Second * 1,
	}

	newConsumer := kafka.NewReader(readerConfig)
	return newConsumer
}

func (k *unsecureCopyKafka) CreateProducer(topic string) *kafka.Writer {
	w := &kafka.Writer{
		Addr:         kafka.TCP(strings.Split(k.s, ",")...),
		Topic:        topic,
		Balancer:     &kafka.RoundRobin{},
		BatchTimeout: 500 * time.Microsecond,
		BatchSize:    1,
		MaxAttempts:  10,
		Async:        false,
		RequiredAcks: kafka.RequireOne,
	}

	return w
}

func (k *unsecureCopyKafka) ReadMessage(consumer *kafka.Reader) (kafka.Message, error) {
	msg, err := consumer.ReadMessage(context.Background())
	return msg, err
}

func (k *unsecureCopyKafka) Stop(consumer *kafka.Reader) {
	_ = consumer.Close()
}

func (k *unsecureCopyKafka) GetPartitions(topicName string) ([]kafka.Partition, error) {
	server := strings.Split(k.s, ",")[0]
	dial, err := kafka.Dial("tcp", server)
	if err == nil {
		partitions, err := dial.ReadPartitions(topicName)
		return partitions, err
	}

	return nil, errors.New("Can not connect kafka host")
}

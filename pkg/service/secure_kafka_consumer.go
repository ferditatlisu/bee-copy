package service

import (
	"bee-copy/pkg/config"
	"bufio"
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/scram"
	"os"
	"strings"
	"time"
)

type CopyKafka interface {
	ReadMessage(consumer *kafka.Reader) (kafka.Message, error)
	Stop(consumer *kafka.Reader)
	GetPartitions(topicName string) ([]kafka.Partition, error)
	CreateConsumer(topic string, partition int) *kafka.Reader
	CreateProducer(topic string) *kafka.Writer
}

type secureCopyKafka struct {
	c *config.KafkaConfig
}

func NewSecureKafkaCopy(c *config.KafkaConfig) CopyKafka {
	return &secureCopyKafka{c}
}

func (k *secureCopyKafka) CreateConsumer(topic string, partition int) *kafka.Reader {
	readerConfig := kafka.ReaderConfig{
		Brokers:   strings.Split(k.c.Host, ","),
		Partition: partition,
		Topic:     topic,
		MinBytes:  10e3, // 10KB
		MaxBytes:  10e6, // 10MB
		MaxWait:   time.Second * 1,
		Dialer:    createKafkaDialer(k.c, createTLSConfig(k.c)),
	}

	newConsumer := kafka.NewReader(readerConfig)
	return newConsumer
}

func (k *secureCopyKafka) CreateProducer(topic string) *kafka.Writer {
	w := &kafka.Writer{
		Addr:         kafka.TCP(strings.Split(k.c.Host, ",")...),
		Topic:        topic,
		Balancer:     &kafka.RoundRobin{},
		BatchTimeout: 500 * time.Microsecond,
		BatchSize:    1,
		MaxAttempts:  10,
		Async:        false,
		RequiredAcks: kafka.RequireOne,
	}

	tlsConfig := createTLSConfig(k.c)
	secureTransport := createKafkaTransport(*k.c.UserName, *k.c.Password, tlsConfig)
	w.Transport = secureTransport

	return w
}

func (k *secureCopyKafka) ReadMessage(consumer *kafka.Reader) (kafka.Message, error) {
	msg, err := consumer.ReadMessage(context.Background())
	return msg, err
}

func (k *secureCopyKafka) Stop(consumer *kafka.Reader) {
	_ = consumer.Close()
}

func (k *secureCopyKafka) GetPartitions(topicName string) ([]kafka.Partition, error) {
	server := strings.Split(k.c.Host, ",")[0]
	dialer := createKafkaDialer(k.c, createTLSConfig(k.c))
	partitions, err := dialer.LookupPartitions(context.Background(), "tcp", server, topicName)
	return partitions, err
}

func createKafkaDialer(kafkaConfig *config.KafkaConfig, tlsConfig *tls.Config) *kafka.Dialer {
	mechanism, err := scram.Mechanism(scram.SHA512, *kafkaConfig.UserName, *kafkaConfig.Password)
	if err != nil {
		panic("Error while creating SCRAM configuration, error: " + err.Error())
	}

	return &kafka.Dialer{
		TLS:           tlsConfig,
		SASLMechanism: mechanism,
	}
}

func createKafkaTransport(userName, password string, tlsConfig *tls.Config) *kafka.Transport {
	mechanism, err := scram.Mechanism(scram.SHA512, userName, password)
	if err != nil {
		panic("Error while creating SCRAM configuration, error: " + err.Error())
	}

	return &kafka.Transport{
		TLS:  tlsConfig,
		SASL: mechanism,
	}
}

func createTLSConfig(kafkaConfig *config.KafkaConfig) *tls.Config {
	path := "resources/rootCA.pem"
	err := createFile(kafkaConfig.Certificate, path)
	if err != nil {
		panic(err.Error())
	}
	rootCA, err := os.ReadFile(path)
	if err != nil {
		panic("Error while reading Root CA file: " + path + " error: " + err.Error())
	}

	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(rootCA)

	return &tls.Config{
		RootCAs:            caCertPool,
		MinVersion:         tls.VersionTLS12,
		InsecureSkipVerify: true,
	}
}

func createFile(lines []string, path string) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()

	w := bufio.NewWriter(file)
	for _, line := range lines {
		fmt.Fprintln(w, line)
	}
	return w.Flush()
}

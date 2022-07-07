// Package provide a Kafka output printer provider for the Cilium event stream

package kafka

import (
	"fmt"
	"github.com/cilium/hubble/pkg/env"
	"strings"

	"github.com/Shopify/sarama"
	"github.com/cilium/hubble/pkg/logger"
)

type ProducerConfiguration struct {
	KafkaBrokers       string
	KafkaUser          string
	KafkaPassword      string
	KafkaVersion       string
	KafkaProducerTopic string
	KafkaMessageKey    string
}

type producer struct {
	asyncProducer sarama.AsyncProducer
	producerTopic string
	key           string
}

type Producer interface {
	Write(p []byte) (n int, err error)
	ClearProducer()
}

func NewAProducer(producerConfig *ProducerConfiguration) (Producer, error) {

	version, err := sarama.ParseKafkaVersion(producerConfig.KafkaVersion)
	if err != nil {
		logger.Logger.WithError(err).Error("Can't parse kafka version")
		return nil, err
	}

	configProducer := sarama.NewConfig()

	configProducer.Version = version
	configProducer.Producer.Partitioner = sarama.NewRandomPartitioner
	configProducer.Producer.Return.Successes = true            // true - need Success channel
	configProducer.Producer.RequiredAcks = sarama.WaitForLocal //default
	configProducer.Producer.MaxMessageBytes = 10000000         // default * 10

	if producerConfig.KafkaUser != "" {
		configProducer.Net.SASL.Enable = true
		configProducer.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA512
		configProducer.Net.SASL.User = producerConfig.KafkaUser
		configProducer.Net.SASL.Password = producerConfig.KafkaPassword
		configProducer.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient {
			return &XDGSCRAMClient{
				Client:             nil,
				ClientConversation: nil,
				HashGeneratorFcn:   SHA512,
			}
		}
	}
	logger.Logger.Infof("Kafkabroker=%s Topic=%s Version=%s",
		producerConfig.KafkaBrokers, producerConfig.KafkaProducerTopic, configProducer.Version)
	asyncProducer, err := sarama.NewAsyncProducer(strings.Split(producerConfig.KafkaBrokers, ","), configProducer)
	if err != nil {
		logger.Logger.WithError(err).Error("Can't create sync producer")
		return nil, err
	}

	return &producer{asyncProducer: asyncProducer, producerTopic: producerConfig.KafkaProducerTopic, key: producerConfig.KafkaMessageKey}, nil
}

func (ap *producer) Write(p []byte) (n int, err error) {
	value := string(p)
	//ap.SendMessage(ap.key, &value)
	msg := &sarama.ProducerMessage{
		Topic: ap.producerTopic,
		Value: sarama.StringEncoder(value),
	}

	if ap.key != "" {
		msg.Key = sarama.StringEncoder(ap.key)
	}

	select {
	case ap.asyncProducer.Input() <- msg:
		logger.Logger.Debug("Produce message")
	case err := <-ap.asyncProducer.Errors():
		logger.Logger.Error("Failed to produce message: ", err)
		if err != nil {
			return 0, err
		}
	case <-ap.asyncProducer.Successes():
		logger.Logger.Debug("Success to produce message")
	}

	return len(p), nil
}

func (ap *producer) ClearProducer() {
	if ap.asyncProducer != nil {
		ap.asyncProducer.Close()
	}
}

func ProducerInit() (Producer, error) {
	producerInfor, err := newProducerConfiguration()
	if err != nil {
		logger.Logger.WithError(err).Error("Can't create producer infor")
		return nil, err
	}

	producer, err := NewAProducer(producerInfor)
	if err != nil {
		logger.Logger.WithError(err).Error("Can't create producer")
		return nil, err
	}

	return producer, nil
}

func newProducerConfiguration() (*ProducerConfiguration, error) {
	kafkaBrokers := env.KafkaBrokers
	if kafkaBrokers == "" {
		return nil, fmt.Errorf("Missing kafka brokers in env")
	}

	kafkaProducerTopic := env.KafkaTopic
	if kafkaProducerTopic == "" {
		return nil, fmt.Errorf("Missing kafka producer topic in env")
	}

	return &ProducerConfiguration{
		KafkaBrokers:       kafkaBrokers,
		KafkaUser:          env.KafkaUser,
		KafkaPassword:      env.KafkaPassword,
		KafkaProducerTopic: kafkaProducerTopic,
		KafkaVersion:       env.KafkaVersion,
		KafkaMessageKey:    env.KafkaMessageKey,
	}, nil
}

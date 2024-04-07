package streaming

import (
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"go.uber.org/zap"
)

type ProducerPort interface {
	SendMessage(topic string, key string, value []byte) error
}

type Producer struct {
	logger           *zap.SugaredLogger
	bootstrapServers string
}

func NewProducer(logger *zap.SugaredLogger, server string) *Producer {
	return &Producer{
		logger:           logger,
		bootstrapServers: server,
	}
}

func (p *Producer) SendMessage(topic string, key string, value []byte) error {
	p.logger.Infof("Sending message to topic: %s", topic)
	prod, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": p.bootstrapServers})
	if err != nil {
		p.logger.With(zap.Error(err)).Error("Failed to create producer")
		return err
	}

	delivery_chan := make(chan kafka.Event, 100)
	defer close(delivery_chan)
	err = prod.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          []byte(value)},
		delivery_chan,
	)
	if err != nil {
		p.logger.With(zap.Error(err)).Error("Failed to produce message")
		return err
	}

	e := <-delivery_chan
	m := e.(*kafka.Message)
	if m.TopicPartition.Error != nil {
		p.logger.Errorf("Delivery failed: %v\n", m.TopicPartition.Error)
	} else {
		p.logger.Infof("Delivered message to topic %s [%d] at offset %v\n",
			*m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Offset)
	}
	return nil
}

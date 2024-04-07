package streaming

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"go.uber.org/zap"
)

type cdcMessage struct {
	Payload struct {
		Before interface{} `json:"before"`
		After  struct {
			ID          string    `json:"id"`
			Destination string    `json:"destination"`
			EventType   string    `json:"event_type"`
			Payload     string    `json:"payload"`
			CreatedAt   time.Time `json:"created_at"`
		} `json:"after"`
		Op string `json:"op"`
	} `json:"payload"`
}

type Consumer struct {
	logger      *zap.SugaredLogger
	kfkConsumer *kafka.Consumer
	producer    ProducerPort
}

func NewConsumer(logger *zap.SugaredLogger, config *kafka.ConfigMap, producer ProducerPort) (*Consumer, error) {
	consumer, err := kafka.NewConsumer(config)
	if err != nil {
		return nil, err
	}

	return &Consumer{
		logger:      logger,
		kfkConsumer: consumer,
		producer:    producer,
	}, nil
}

func (c *Consumer) Start(ctx context.Context, topics []string) error {
	defer func() {
		c.logger.Info("Consumer deferred")
		c.kfkConsumer.Close()
	}()
	c.logger.Infof("Subscribing to topics: %v", topics)
	err := c.kfkConsumer.SubscribeTopics(topics, nil)
	if err != nil {
		return err
	}
	c.logger.Info("Subscribed to topics")

	run := true
	for run {
		select {
		case <-ctx.Done():
			c.logger.Info("Context Cancelled")
			return nil
		default:
			ev := c.kfkConsumer.Poll(100)
			if ev == nil {
				continue
			}
			switch e := ev.(type) {
			case *kafka.Message:
				// Process the message received.
				var cdcMessage cdcMessage
				err := json.Unmarshal(e.Value, &cdcMessage)
				if err != nil {
					c.logger.With(zap.Error(err)).Error("Failed to decode message")
					return err
				}
				if cdcMessage.Payload.Op != "c" {
					c.logger.Infof("Ignoring operation %s", cdcMessage.Payload.Op)
					_, err = c.kfkConsumer.StoreMessage(e)
					if err != nil {
						c.logger.With(zap.Error(err)).Error("Failed to store message")
						return err
					}
				}

				c.logger.Infof("Processing message %s", cdcMessage.Payload.After.ID)
				var data map[string]interface{}
				err = json.Unmarshal([]byte(cdcMessage.Payload.After.Payload), &data)
				if err != nil {
					c.logger.With(zap.Error(err)).Error("Failed to decode payload")
					return err
				}

				payloadData, err := json.Marshal(data)
				if err != nil {
					c.logger.With(zap.Error(err)).Error("Failed to encode payload")
					return err
				}

				err = c.producer.SendMessage(cdcMessage.Payload.After.Destination, cdcMessage.Payload.After.ID, payloadData)
				if err != nil {
					c.logger.With(zap.Error(err)).Error("Failed to send message")
					return err
				}

				_, err = c.kfkConsumer.StoreMessage(e)
				if err != nil {
					c.logger.With(zap.Error(err)).Error("Failed to commit message")
					return err
				}
			case kafka.Error:
				c.logger.With(zap.Error(e)).Error("Consumer error")
				return e
			default:
				fmt.Printf("Ignored %v\n", e)
			}
		}
	}
	c.logger.Info("Run finished")
	return nil
}

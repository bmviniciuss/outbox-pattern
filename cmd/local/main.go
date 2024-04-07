package main

import (
	"context"
	"os"
	"os/signal"
	"syscall"

	"github.com/bmviniciuss/outbox-pattern-go/internal/adapters/streaming"
	"github.com/bmviniciuss/outbox-pattern-go/internal/config/logger"
	"github.com/confluentinc/confluent-kafka-go/v2/kafka"
	"go.uber.org/zap"
)

func main() {
	var (
		ctx, cancel = context.WithCancel(context.Background())
		sigchan     = make(chan os.Signal, 1)
	)
	defer cancel()
	defer close(sigchan)

	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	logger := logger.NewLogger()
	logger.Info("Hello World")

	go func() {
		<-sigchan
		logger.Infof("Received signal to shutdown")
		cancel()
	}()

	cfg := &kafka.ConfigMap{
		"bootstrap.servers":        "localhost:9092",
		"broker.address.family":    "v4",
		"group.id":                 "outbox-worker",
		"session.timeout.ms":       6000,
		"auto.offset.reset":        "earliest",
		"enable.auto.offset.store": false,
		"enable.auto.commit":       true,
	}

	producer := streaming.NewProducer(logger, "localhost:9092")
	consumer, err := streaming.NewConsumer(logger, cfg, producer)
	if err != nil {
		logger.With(zap.Error(err)).Fatal("Failed to create consumer")
	}

	topics := []string{"outbox.outbox.messages"}
	err = consumer.Start(ctx, topics)
	if err != nil {
		logger.With(zap.Error(err)).Fatal("Failed to start consumer")
	}

}

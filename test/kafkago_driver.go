package test

import (
	"context"
	"fmt"
	"time"

	bgKafka "github.com/netcracker/qubership-core-lib-go-bg-kafka/v3"
	"github.com/netcracker/qubership-core-lib-go-bg-kafka/v3/test/kafkago"
	kafkagodriver "github.com/segmentio/kafka-go"
)

// This file wires the kafkago adapter package (./kafkago) into the reusable RunBGConsumerTest /
// RunBGConsumerVersionNameTest harnesses so they can run against a real broker (spun up via
// testcontainers in StartContainers).

func newBrokerClient(brokers []string) *kafkagodriver.Client {
	return &kafkagodriver.Client{Addr: kafkagodriver.TCP(brokers...)}
}

func createTopicFunc(client *kafkagodriver.Client, topic string) func(replicationFactor int) error {
	return func(replicationFactor int) error {
		resp, err := client.CreateTopics(context.Background(), &kafkagodriver.CreateTopicsRequest{
			Topics: []kafkagodriver.TopicConfig{{Topic: topic, NumPartitions: 1, ReplicationFactor: replicationFactor}},
		})
		if err != nil {
			return err
		}
		for name, topicErr := range resp.Errors {
			if topicErr != nil {
				return fmt.Errorf("failed to create topic '%s': %w", name, topicErr)
			}
		}
		return nil
	}
}

func writerSupplierFunc(brokers []string, topic string) func() (Writer, error) {
	return func() (Writer, error) {
		return &kafkago.Writer{W: &kafkagodriver.Writer{
			Addr:         kafkagodriver.TCP(brokers...),
			Topic:        topic,
			Balancer:     &kafkagodriver.LeastBytes{},
			RequiredAcks: kafkagodriver.RequireAll,
		}}, nil
	}
}

func consumerSupplierFunc(brokers []string, topic string) func(groupId string) (bgKafka.Consumer, error) {
	return func(groupId string) (bgKafka.Consumer, error) {
		reader := kafkagodriver.NewReader(kafkagodriver.ReaderConfig{
			Brokers:        brokers,
			GroupID:        groupId,
			Topic:          topic,
			CommitInterval: 0, // commit manually via CommitMessages
			MaxWait:        500 * time.Millisecond,
			StartOffset:    kafkagodriver.FirstOffset, // only used when the group has no committed offset yet
		})
		return &kafkago.Consumer{Reader: reader}, nil
	}
}

func bgConsumerSupplierFunc(ctx context.Context, brokers []string, topic, groupPrefix string, client *kafkagodriver.Client) func(options []bgKafka.Option) (*bgKafka.BgConsumer, error) {
	dialer := kafkagodriver.DefaultDialer
	adminSupplier := func() (bgKafka.NativeAdminAdapter, error) {
		return kafkago.NewAdmin(client.Addr, client, dialer, brokers, topic), nil
	}
	consumerSupplier := consumerSupplierFunc(brokers, topic)
	return func(options []bgKafka.Option) (*bgKafka.BgConsumer, error) {
		return bgKafka.NewConsumer(ctx, topic, groupPrefix, adminSupplier, consumerSupplier, options...)
	}
}

package test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

// TestRunBGConsumer runs the full BG consumer scenario against a real Kafka broker
// (testcontainers) using the segmentio/kafka-go driver adapter.
func TestRunBGConsumer(t *testing.T) {
	brokers, err := StartContainers(t)
	require.NoError(t, err)

	ctx := context.Background()
	topic := "orders"
	groupPrefix := "bg-consumer"
	client := newBrokerClient(brokers)

	RunBGConsumerTest(t, topic,
		bgConsumerSupplierFunc(ctx, brokers, topic, groupPrefix, client),
		writerSupplierFunc(brokers, topic),
		createTopicFunc(client, topic),
	)
}

// TestRunBGConsumerVersionName runs the X-Version-Name routing scenario (with fallback to
// X-Version) against a real Kafka broker.
func TestRunBGConsumerVersionName(t *testing.T) {
	brokers, err := StartContainers(t)
	require.NoError(t, err)

	ctx := context.Background()
	topic := "orders-vname"
	groupPrefix := "bg-consumer-vname"
	client := newBrokerClient(brokers)

	RunBGConsumerVersionNameTest(t, topic,
		bgConsumerSupplierFunc(ctx, brokers, topic, groupPrefix, client),
		writerSupplierFunc(brokers, topic),
		createTopicFunc(client, topic),
	)
}

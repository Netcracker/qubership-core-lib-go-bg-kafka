// Package kafkago wraps github.com/segmentio/kafka-go into the bg-kafka native interfaces
// (Message, Consumer, NativeAdminAdapter), used by the integration test harness under test/.
package kafkago

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"time"

	bgKafka "github.com/netcracker/qubership-core-lib-go-bg-kafka/v3"
	"github.com/netcracker/qubership-core-lib-go/v3/logging"
	"github.com/segmentio/kafka-go"
)

var logger logging.Logger

func init() {
	logger = logging.GetLogger("bg-kafka-kafkago")
}

// ---------------------------------------------------------------------------
// Message adapter
// ---------------------------------------------------------------------------

type Message struct {
	native *kafka.Message
}

func FromKafkaMessage(native *kafka.Message) *Message {
	return &Message{native: native}
}

func (m *Message) Topic() string        { return m.native.Topic }
func (m *Message) Offset() int64        { return m.native.Offset }
func (m *Message) HighWaterMark() int64 { return m.native.HighWaterMark }
func (m *Message) Key() []byte          { return m.native.Key }
func (m *Message) Value() []byte        { return m.native.Value }
func (m *Message) Partition() int       { return m.native.Partition }
func (m *Message) NativeMsg() any       { return m.native }

func (m *Message) Headers() []bgKafka.Header {
	headers := make([]bgKafka.Header, 0, len(m.native.Headers))
	for _, h := range m.native.Headers {
		headers = append(headers, bgKafka.Header{Key: h.Key, Value: h.Value})
	}
	return headers
}

// ---------------------------------------------------------------------------
// Consumer adapter (kafka-go Reader with a consumer group)
// ---------------------------------------------------------------------------

type Consumer struct {
	Reader *kafka.Reader
}

func (c *Consumer) Offset() int64 {
	return c.Reader.Offset()
}

func (c *Consumer) ReadMessage(ctx context.Context) (bgKafka.Message, error) {
	native, err := c.Reader.FetchMessage(ctx)
	if err != nil {
		return nil, err
	}
	return FromKafkaMessage(&native), nil
}

func (c *Consumer) Commit(ctx context.Context, marker *bgKafka.CommitMarker) error {
	return c.Reader.CommitMessages(ctx, kafka.Message{
		Topic:     marker.TopicPartition.Topic,
		Partition: marker.TopicPartition.Partition,
		Offset:    marker.OffsetAndMeta.Offset,
	})
}

func (c *Consumer) Close() error {
	return c.Reader.Close()
}

// ---------------------------------------------------------------------------
// Writer adapter (sync producer)
// ---------------------------------------------------------------------------

type Writer struct {
	W *kafka.Writer
}

func (w *Writer) Write(ctx context.Context, message bgKafka.Message) error {
	headers := make([]kafka.Header, 0, len(message.Headers()))
	for _, h := range message.Headers() {
		headers = append(headers, kafka.Header{Key: h.Key, Value: h.Value})
	}
	return w.W.WriteMessages(ctx, kafka.Message{
		Key:     message.Key(),
		Value:   message.Value(),
		Headers: headers,
	})
}

// ---------------------------------------------------------------------------
// Admin adapter
// ---------------------------------------------------------------------------

type Dialer interface {
	Dial(network string, address string) (*kafka.Conn, error)
}

type Client interface {
	ListGroups(ctx context.Context, req *kafka.ListGroupsRequest) (*kafka.ListGroupsResponse, error)
	CreateTopics(ctx context.Context, req *kafka.CreateTopicsRequest) (*kafka.CreateTopicsResponse, error)
	OffsetFetch(ctx context.Context, req *kafka.OffsetFetchRequest) (*kafka.OffsetFetchResponse, error)
	OffsetCommit(ctx context.Context, req *kafka.OffsetCommitRequest) (*kafka.OffsetCommitResponse, error)
	ListOffsets(ctx context.Context, req *kafka.ListOffsetsRequest) (*kafka.ListOffsetsResponse, error)
}

type Admin struct {
	Addr    net.Addr
	Client  Client
	Dialer  Dialer
	Brokers []string
	Topic   string
}

func NewAdmin(addr net.Addr, client Client, dialer Dialer, brokers []string, topic string) *Admin {
	return &Admin{Addr: addr, Client: client, Dialer: dialer, Brokers: brokers, Topic: topic}
}

func (a *Admin) ListConsumerGroups(ctx context.Context) (result []bgKafka.ConsumerGroup, err error) {
	listGroupsResponse, err := a.Client.ListGroups(ctx, &kafka.ListGroupsRequest{})
	if err != nil {
		return
	}
	for _, gr := range listGroupsResponse.Groups {
		result = append(result, bgKafka.ConsumerGroup{GroupId: gr.GroupID})
	}
	return
}

func (a *Admin) ListConsumerGroupOffsets(ctx context.Context, groupId string) (map[bgKafka.TopicPartition]bgKafka.OffsetAndMetadata, error) {
	partitions, err := a.PartitionsFor(ctx, a.Topic)
	if err != nil {
		return nil, err
	}
	var topicPartitions map[string][]int
	for _, p := range partitions {
		if topicPartitions == nil {
			topicPartitions = map[string][]int{}
		}
		parts := topicPartitions[p.Topic]
		parts = append(parts, p.Partition)
		topicPartitions[p.Topic] = parts
	}
	offsetFetchResponse, err := a.Client.OffsetFetch(ctx, &kafka.OffsetFetchRequest{
		GroupID: groupId,
		Topics:  topicPartitions,
	})
	if err != nil {
		return nil, err
	}
	result := map[bgKafka.TopicPartition]bgKafka.OffsetAndMetadata{}
	for t, ofps := range offsetFetchResponse.Topics {
		for _, ofp := range ofps {
			result[bgKafka.TopicPartition{
				Partition: ofp.Partition,
				Topic:     t,
			}] = bgKafka.OffsetAndMetadata{Offset: ofp.CommittedOffset}
		}
	}
	return result, err
}

func (a *Admin) AlterConsumerGroupOffsets(ctx context.Context, groupId bgKafka.GroupId, proposedOffsets map[bgKafka.TopicPartition]bgKafka.OffsetAndMetadata) error {
	topicOffsets := map[string][]kafka.OffsetCommit{}
	var topics []string
	for tp, om := range proposedOffsets {
		offsetCommitsForTopic := topicOffsets[tp.Topic]
		offsetCommitsForTopic = append(offsetCommitsForTopic, kafka.OffsetCommit{
			Partition: tp.Partition,
			Offset:    om.Offset,
		})
		topicOffsets[tp.Topic] = offsetCommitsForTopic
		topics = append(topics, tp.Topic)
	}
	// need to join consumer group to avoid NotCoordinatorForGroup(16) error from Kafka
	consumerGroupConfig := kafka.ConsumerGroupConfig{
		ID:      groupId.String(),
		Brokers: a.Brokers,
		Dialer:  a.Dialer.(*kafka.Dialer),
		Topics:  topics,
	}
	return getConsumerGroupGenerationWithRetry(ctx, consumerGroupConfig, func(ctx context.Context, generation *kafka.Generation) error {
		response, err := a.Client.OffsetCommit(ctx, &kafka.OffsetCommitRequest{
			GroupID:      groupId.String(),
			Topics:       topicOffsets,
			GenerationID: int(generation.ID),
			MemberID:     generation.MemberID,
		})
		if err != nil {
			return err
		}
		var errs []error
		for _, tErr := range response.Topics {
			for _, pErr := range tErr {
				if pErr.Error != nil {
					errs = append(errs, pErr.Error)
				}
			}
		}
		if len(errs) > 0 {
			return fmt.Errorf("errors: %v", errs)
		}
		return nil
	})
}

func getConsumerGroupGenerationWithRetry(ctx context.Context, config kafka.ConsumerGroupConfig,
	commitOperation func(ctx context.Context, generation *kafka.Generation) error) error {
	var err error
	var consumerGroup *kafka.ConsumerGroup
	warnLogFunc := func(ctx context.Context, id string, err error) {
		logger.DebugC(ctx, "Failed to get next generation for consumer group: '%s', err: %s", config.ID, err.Error())
	}
	sleepDuration := 100 * time.Millisecond
	attempts := 100
	attempt := 0
	for attempt < attempts {
		attempt++
		consumerGroup, err = kafka.NewConsumerGroup(config)
		if err != nil {
			if errors.Is(err, ctx.Err()) {
				return err
			}
			warnLogFunc(ctx, config.ID, err)
			time.Sleep(sleepDuration)
			continue
		}
		err = getConsumerGroupGenerationAndCommit(ctx, consumerGroup, commitOperation)
		if err != nil {
			if errors.Is(err, ctx.Err()) {
				return err
			}
			warnLogFunc(ctx, config.ID, err)
			time.Sleep(sleepDuration)
			continue
		}
		return nil
	}
	return err
}

func getConsumerGroupGenerationAndCommit(ctx context.Context, consumerGroup *kafka.ConsumerGroup,
	commitOperation func(ctx context.Context, generation *kafka.Generation) error) error {
	generation, err := consumerGroup.Next(ctx)
	defer consumerGroup.Close()
	if err != nil {
		return err
	}
	return commitOperation(ctx, generation)
}

func (a *Admin) PartitionsFor(ctx context.Context, topics ...string) (partitionsInfo []bgKafka.PartitionInfo, err error) {
	if len(topics) == 0 {
		err = errors.New("topics cannot be empty")
		return
	}
	conn, err := a.connectToAnyBroker(ctx)
	if err != nil {
		return
	}
	defer conn.Close()
	partitions, err := conn.ReadPartitions(topics...)
	if err != nil {
		return
	}
	for _, p := range partitions {
		partitionsInfo = append(partitionsInfo, bgKafka.PartitionInfo{
			Topic:     p.Topic,
			Partition: p.ID,
		})
	}
	return
}

func (a *Admin) connectToAnyBroker(ctx context.Context) (conn *kafka.Conn, err error) {
	networks := strings.Split(a.Addr.Network(), ",")
	addresses := strings.Split(a.Addr.String(), ",")
	if len(networks) != len(addresses) {
		return nil, fmt.Errorf("invalid client Addr: Network() must return the same entries as String(), but got: '%v' and '%v'",
			networks, addresses)
	}
	brokers := map[string]string{}
	for i, network := range networks {
		brokers[network] = addresses[i]
	}
	for network, broker := range brokers {
		// connect to the first available broker
		if conn, err = a.Dialer.Dial(network, broker); err == nil {
			return conn, nil
		}
	}
	return
}

func (a *Admin) BeginningOffsets(ctx context.Context, topicPartitions []bgKafka.TopicPartition) (map[bgKafka.TopicPartition]int64, error) {
	return a.absoluteOffsets(ctx, topicPartitions,
		func(partition int) kafka.OffsetRequest {
			return kafka.FirstOffsetOf(partition)
		},
		func(offsets kafka.PartitionOffsets) (int64, time.Time) {
			return offsets.FirstOffset, time.Time{}
		})
}

func (a *Admin) EndOffsets(ctx context.Context, topicPartitions []bgKafka.TopicPartition) (map[bgKafka.TopicPartition]int64, error) {
	return a.absoluteOffsets(ctx, topicPartitions,
		func(partition int) kafka.OffsetRequest {
			return kafka.LastOffsetOf(partition)
		},
		func(offsets kafka.PartitionOffsets) (int64, time.Time) {
			return offsets.LastOffset, time.Time{}
		})
}

func (a *Admin) absoluteOffsets(ctx context.Context, topicPartitions []bgKafka.TopicPartition,
	reqFunc func(partition int) kafka.OffsetRequest,
	offsetFunc func(offsets kafka.PartitionOffsets) (int64, time.Time)) (map[bgKafka.TopicPartition]int64, error) {
	offsetRequests := make(map[bgKafka.TopicPartition]kafka.OffsetRequest)
	for _, tp := range topicPartitions {
		offsetRequests[tp] = reqFunc(tp.Partition)
	}
	offsetForTimes, err := a.offsetsForTimes(ctx, offsetRequests, offsetFunc)
	result := map[bgKafka.TopicPartition]int64{}
	for tp, ot := range offsetForTimes {
		result[tp] = ot.Offset
	}
	return result, err
}

func (a *Admin) OffsetsForTimes(ctx context.Context, m map[bgKafka.TopicPartition]time.Time) (
	map[bgKafka.TopicPartition]*bgKafka.OffsetAndTimestamp, error) {
	offsetRequests := make(map[bgKafka.TopicPartition]kafka.OffsetRequest)
	for tp, t := range m {
		offsetRequests[tp] = kafka.TimeOffsetOf(tp.Partition, t)
	}
	return a.offsetsForTimes(ctx, offsetRequests, func(offsets kafka.PartitionOffsets) (int64, time.Time) {
		offset := int64(0)
		timestamp := time.Time{}
		for offs, t := range offsets.Offsets {
			if timestamp.IsZero() || t.Before(timestamp) {
				timestamp = t
				offset = offs
			}
		}
		return offset, timestamp
	})
}

func (a *Admin) offsetsForTimes(ctx context.Context, m map[bgKafka.TopicPartition]kafka.OffsetRequest,
	offsetFunc func(offsets kafka.PartitionOffsets) (int64, time.Time)) (
	result map[bgKafka.TopicPartition]*bgKafka.OffsetAndTimestamp, err error) {
	topicsMap := map[string][]kafka.OffsetRequest{}
	for tp, offsetRequest := range m {
		topicsMap[tp.Topic] = []kafka.OffsetRequest{offsetRequest}
	}
	offsetsResponse, err := a.Client.ListOffsets(ctx, &kafka.ListOffsetsRequest{
		Topics: topicsMap,
	})
	if err != nil {
		return
	}
	result = map[bgKafka.TopicPartition]*bgKafka.OffsetAndTimestamp{}
	for t, pos := range offsetsResponse.Topics {
		for _, po := range pos {
			topicPartition := bgKafka.TopicPartition{
				Partition: po.Partition,
				Topic:     t,
			}
			offset, timestamp := offsetFunc(po)
			result[topicPartition] = &bgKafka.OffsetAndTimestamp{
				Offset:    offset,
				Timestamp: timestamp.UnixMilli(),
			}
		}
	}
	return
}

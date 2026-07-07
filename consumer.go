package blue_green_kafka

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	bgMonitor "github.com/netcracker/qubership-core-lib-go-bg-state-monitor/v2"
	"github.com/netcracker/qubership-core-lib-go/v3/context-propagation/baseproviders/xversion"
	"github.com/netcracker/qubership-core-lib-go/v3/context-propagation/baseproviders/xversionname"
	"github.com/netcracker/qubership-core-lib-go/v3/logging"
)

var logger logging.Logger
var ConsumerReadyTimeout = 30 * time.Second

func init() {
	logger = logging.GetLogger("maas-bg-kafka-consumer")
}

type BgConsumer struct {
	Config            *BgKafkaConsumerConfig
	groupPrefix       string
	statePublisher    BGStatePublisher
	consumer          Consumer
	bgStateChange     *atomic.Pointer[bgMonitor.BlueGreenState]
	bgStateActive     *bgMonitor.BlueGreenState
	versionFilter     *Filter
	versionNameFilter *Filter
	pollMux           *sync.Mutex
	metrics           *Metrics
}

func NewConsumer(ctx context.Context, topic, groupIdPrefix string,
	adminSupplier func() (NativeAdminAdapter, error),
	consumerSupplier func(groupId string) (Consumer, error),
	opts ...Option) (*BgConsumer, error) {
	config := &BgKafkaConsumerConfig{
		Topic:                        func() string { return topic },
		GroupIdPrefix:                func() string { return groupIdPrefix },
		AdminSupplier:                adminSupplier,
		ActiveOffsetSetupStrategy:    func() OffsetSetupStrategy { return Rewind(time.Minute * 5) },
		CandidateOffsetSetupStrategy: func() OffsetSetupStrategy { return Rewind(time.Minute * 5) },
		ConsumerSupplier:             consumerSupplier,
		ConsistencyMode:              func() ConsumerConsistencyMode { return Eventual },
		BlueGreenStatePublisher: func() (BGStatePublisher, error) {
			namespace := getNamespace()
			consulUrl := getConsulUrl()
			consulTokenSupplier, err := getConsulTokenSupplier(ctx, consulUrl, namespace)
			if err != nil {
				return nil, err
			}
			return bgMonitor.NewPublisher(ctx, consulUrl, namespace, consulTokenSupplier)
		},
		ReadTimeout: func() time.Duration { return time.Minute },
	}
	for _, option := range opts {
		option(config)
	}
	statePublisher, err := config.BlueGreenStatePublisher()
	if err != nil {
		return nil, fmt.Errorf("failed to create consumer: %w", err)
	}
	bgConsumer := &BgConsumer{
		Config:         config,
		groupPrefix:    config.GroupIdPrefix(),
		bgStateChange:  &atomic.Pointer[bgMonitor.BlueGreenState]{},
		statePublisher: statePublisher,
		pollMux:        &sync.Mutex{},
		metrics:        NewConsumerMetrics(topic),
	}
	readyChan := make(chan struct{}, 1)
	once := &sync.Once{}
	statePublisher.Subscribe(ctx, func(state bgMonitor.BlueGreenState) {
		bgConsumer.bgStateChange.Store(&state)
		once.Do(func() {
			readyChan <- struct{}{}
		})
	})
	//start background check for ctx.Done() to trigger consumer close() func
	go func() {
		select {
		case <-ctx.Done():
			logger.InfoC(ctx, "Ctx is done, closing bgConsumer")
			bgConsumer.consumer.Close()
		}
	}()
	timer := time.NewTimer(ConsumerReadyTimeout)
	select {
	case <-readyChan:
		timer.Stop()
		return bgConsumer, nil
	case <-timer.C:
		return nil, errors.New("timed out to wait for consumer to get ready")
	}
}

type Record struct {
	Message Message
	Marker  *CommitMarker
}

type RecordsBatch struct {
	Records []Record
	Marker  *CommitMarker
}

type CommitMarker struct {
	Version        bgMonitor.NamespaceVersion
	TopicPartition TopicPartition
	OffsetAndMeta  OffsetAndMetadata
}

type PartitionInfo struct {
	Topic     string
	Partition int
}

type OffsetAndTimestamp struct {
	Offset    int64
	Timestamp int64
}

func (c *BgConsumer) Commit(ctx context.Context, marker *CommitMarker) error {
	var current *bgMonitor.NamespaceVersion
	if c.bgStateActive != nil {
		current = &c.bgStateActive.Current
	}
	if current != nil && *current == marker.Version {
		logger.DebugC(ctx, "Committing message with marker: %s", marker.String())
		if err := c.consumer.Commit(ctx, marker); err != nil {
			return fmt.Errorf("failed to commit offset: %w", err)
		}

		c.metrics.updateMetricsOnCommit(&commitMetricsUpdate{
			partition:    marker.TopicPartition.Partition,
			commitOffset: marker.OffsetAndMeta.Offset,
		})

		return nil
	}
	logger.WarnC(ctx, "Ignoring commit of message with marker: %s"+
		"current bg state's version: %s", marker.String(), current.String())
	return nil
}

func (c *BgConsumer) Poll(ctx context.Context, readTimeout time.Duration) (*Record, error) {
	c.pollMux.Lock()
	defer c.pollMux.Unlock()
	state := c.bgStateChange.Swap(nil)
	if state != nil {
		logger.InfoC(ctx, "Re initializing consumer for state: %+v", state)
		var err error
		c.bgStateActive = state
		if c.consumer != nil {
			// close prev consumer
			cErr := c.consumer.Close()
			logger.InfoC(ctx, "Closed previous consumer. Err: %v", cErr)
		}
		c.consumer, c.versionFilter, c.versionNameFilter, err = c.reinitializeConsumer(ctx, *state, c.Config)
		if err != nil {
			c.bgStateChange.Store(state)
			return nil, fmt.Errorf("failed to create consumer: %w", err)
		}
	}
	ctx, cancelF := context.WithTimeout(ctx, readTimeout)
	defer cancelF()
	logger.DebugC(ctx, "ReadMessage with timeout: %s", readTimeout.String())
	msg, err := c.consumer.ReadMessage(ctx)
	if err != nil {
		logger.DebugC(ctx, "ReadMessage returned err: %s", err.Error())
		if errors.Is(err, context.DeadlineExceeded) {
			return nil, err
		}
		return nil, fmt.Errorf("failed to read message: %w", err)
	}
	msgKey := string(msg.Key())
	logger.DebugC(ctx, "ReadMessage returned message: topic: '%s', partition: %d, offset: %d, key: '%s'",
		msg.Topic(), msg.Partition(), msg.Offset(), msgKey)
	marker := CommitMarker{
		Version:        c.bgStateActive.Current,
		TopicPartition: TopicPartition{Topic: msg.Topic(), Partition: msg.Partition()},
		OffsetAndMeta:  OffsetAndMetadata{Offset: msg.Offset()},
	}
	xVer := getXVersion(msg.Headers())
	xVerName := getXVersionName(msg.Headers())
	// prefer the version-name header; fall back to the legacy version-number filter when it is absent
	var accepted bool
	if xVerName != "" {
		accepted, err = c.versionNameFilter.Test(xVerName)
		if err != nil {
			return nil, fmt.Errorf("invalid '%s' value format: %s", xversionname.X_VERSION_NAME_CONTEXT_NAME, xVerName)
		}
	} else {
		accepted, err = c.versionFilter.Test(xVer)
		if err != nil {
			// invalid version format
			return nil, fmt.Errorf("invalid '%s' value format: %s", xversion.X_VERSION_HEADER_NAME, xVer)
		}
	}

	c.metrics.updateMetricsOnPoll(&pollMetricsUpdate{
		partition:     msg.Partition(),
		highWaterMark: msg.HighWaterMark(),
		currentOffset: msg.Offset(),
		isAccepted:    accepted,
	})
	if accepted {
		logger.DebugC(ctx, "Message (key='%s') '%s'='%s', '%s'='%s' is accepted by the filter", msgKey, xversionname.X_VERSION_NAME_CONTEXT_NAME, xVerName, xversion.X_VERSION_HEADER_NAME, xVer)
		return &Record{Message: msg, Marker: &marker}, nil
	} else {
		logger.DebugC(ctx, "Message (key='%s') '%s'='%s', '%s'='%s' is declined by the filter", msgKey, xversionname.X_VERSION_NAME_CONTEXT_NAME, xVerName, xversion.X_VERSION_HEADER_NAME, xVer)
		return &Record{Message: nil, Marker: &marker}, nil
	}
}

// Stats returns blue-green kafka consumer metrics snapshot
func (c *BgConsumer) Stats() Metrics {
	return c.metrics.snapshot()
}

func (c *BgConsumer) reinitializeConsumer(ctx context.Context, bgState bgMonitor.BlueGreenState, cfg *BgKafkaConsumerConfig) (Consumer, *Filter, *Filter, error) {
	current := bgState.Current
	var siblingNsState *bgMonitor.State
	if bgState.Sibling != nil {
		siblingNsState = &bgState.Sibling.State
	}
	var groupId GroupId
	if siblingNsState == nil {
		groupId = &PlainGroupId{Name: c.groupPrefix}
	} else {
		groupId = &VersionedGroupId{
			GroupIdPrefix: c.groupPrefix,
			Version:       *current.Version,
			State:         current.State,
			SiblingState:  *siblingNsState,
			Updated:       bgState.UpdateTime,
		}
	}
	logger.InfoC(ctx, "Initializing with groupId: %s", groupId.String())
	offsetMng := offsetManager{config: *c.Config}
	err := offsetMng.alignOffset(ctx, groupId)
	if err != nil {
		return nil, nil, nil, err
	}
	versionFilter := NewFilter(bgState)
	versionNameFilter := NewVersionNameFilter(bgState)
	logger.InfoC(ctx, "initialized version filter: %s, version-name filter: %s", versionFilter.Presentation, versionNameFilter.Presentation)
	groupIdStr := groupId.String()
	consumer, err := cfg.ConsumerSupplier(groupIdStr)
	if err != nil {
		return nil, nil, nil, err
	}
	c.metrics.clean()
	return consumer, versionFilter, versionNameFilter, nil
}

func getXVersion(headers []Header) string {
	return getHeader(headers, xversion.X_VERSION_HEADER_NAME)
}

func getXVersionName(headers []Header) string {
	return getHeader(headers, xversionname.X_VERSION_NAME_CONTEXT_NAME)
}

func getHeader(headers []Header, key string) string {
	for _, h := range headers {
		if strings.EqualFold(h.Key, key) {
			return string(h.Value)
		}
	}
	return ""
}

func (m *CommitMarker) String() string {
	if m == nil {
		return "nil"
	}
	return fmt.Sprintf("Topic: %s, Partition: %v, Offset: %v, Version: %s",
		m.TopicPartition.Topic, m.TopicPartition.Partition, m.OffsetAndMeta.Offset, m.Version.String())
}

func (o *OffsetAndTimestamp) String() string {
	if o == nil {
		return "nil"
	}
	return fmt.Sprintf("Offset: %v, Timestamp: %v", o.Offset, o.Timestamp)
}

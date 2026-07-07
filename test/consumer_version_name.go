package test

import (
	"context"
	"os"
	"testing"
	"time"

	bgKafka "github.com/netcracker/qubership-core-lib-go-bg-kafka/v3"
	bgStateMonitor "github.com/netcracker/qubership-core-lib-go-bg-state-monitor/v2"
	"github.com/netcracker/qubership-core-lib-go/v3/configloader"
	"github.com/netcracker/qubership-core-lib-go/v3/context-propagation/baseproviders"
	"github.com/netcracker/qubership-core-lib-go/v3/context-propagation/baseproviders/xversion"
	"github.com/netcracker/qubership-core-lib-go/v3/context-propagation/baseproviders/xversionname"
	"github.com/netcracker/qubership-core-lib-go/v3/context-propagation/ctxmanager"
	"github.com/stretchr/testify/require"
)

// RunBGConsumerVersionNameTest verifies end-to-end routing by the X-Version-Name header, with a
// fallback to the legacy X-Version filter when the header is absent.
//
// Records produced (single partition, in order):
//
//	key 1: X-Version-Name=active     -> new name filter
//	key 2: X-Version-Name=candidate  -> new name filter
//	key 3: no name, X-Version=v2     -> fallback to legacy version filter
//	key 4: no headers                -> fallback to legacy version filter (empty version)
//
// ACTIVE consumer (sibling CANDIDATE): name filter '!candidate', fallback '!v2'  -> accepts 1, 4
// CANDIDATE consumer (sibling ACTIVE): name filter 'candidate',  fallback '==v2' -> accepts 2, 3
func RunBGConsumerVersionNameTest(t *testing.T, topic string,
	bgConsumerSupplier func(options []bgKafka.Option) (*bgKafka.BgConsumer, error),
	writerSupplier func() (Writer, error),
	createTopic func(replicationFactor int) error) {
	initialLogLevel := os.Getenv("LOG_LEVEL")
	defer os.Setenv("LOG_LEVEL", initialLogLevel)
	os.Setenv("LOG_LEVEL", "debug")

	ctx := context.Background()
	assertions := require.New(t)
	configloader.InitWithSourcesArray([]*configloader.PropertySource{configloader.EnvPropertySource()})
	ctxmanager.Register(baseproviders.Get())

	updateTime := time.Date(2018, 12, 3, 19, 34, 50, 0, time.UTC)
	bgStateActive := bgStateMonitor.BlueGreenState{
		Current:    bgStateMonitor.NamespaceVersion{Namespace: namespace1, Version: v1, State: bgStateMonitor.StateActive},
		Sibling:    &bgStateMonitor.NamespaceVersion{Namespace: namespace2, Version: v2, State: bgStateMonitor.StateCandidate},
		UpdateTime: updateTime,
	}
	bgStateCandidate := bgStateMonitor.BlueGreenState{
		Current:    bgStateMonitor.NamespaceVersion{Namespace: namespace2, Version: v2, State: bgStateMonitor.StateCandidate},
		Sibling:    &bgStateMonitor.NamespaceVersion{Namespace: namespace1, Version: v1, State: bgStateMonitor.StateActive},
		UpdateTime: updateTime,
	}

	statePublisherActive, err := bgStateMonitor.NewInMemoryPublisher(bgStateActive)
	assertions.NoError(err)
	statePublisherCandidate, err := bgStateMonitor.NewInMemoryPublisher(bgStateCandidate)
	assertions.NoError(err)

	consumerActive, err := bgConsumerSupplier([]bgKafka.Option{
		bgKafka.WithBlueGreenStatePublisher(statePublisherActive),
		bgKafka.WithConsistencyMode(bgKafka.GuaranteeConsumption),
	})
	assertions.NoError(err)

	consumerCandidate, err := bgConsumerSupplier([]bgKafka.Option{
		bgKafka.WithBlueGreenStatePublisher(statePublisherCandidate),
		bgKafka.WithConsistencyMode(bgKafka.GuaranteeConsumption),
	})
	assertions.NoError(err)

	err = createTopic(1)
	assertions.NoError(err)
	logger.InfoC(ctx, "created topic '%s'", topic)

	writer, err := writerSupplier()
	assertions.NoError(err)

	// send lowercase names on purpose to also exercise case-insensitive matching against the
	// uppercase bg state names (ACTIVE/CANDIDATE)
	writeMessage(assertions, ctx, writer,
		newMessage("1", "order#1", newHeader(xversionname.X_VERSION_NAME_CONTEXT_NAME, "active")),
		newMessage("2", "order#2", newHeader(xversionname.X_VERSION_NAME_CONTEXT_NAME, "candidate")),
		newMessage("3", "order#3", newHeader(xversion.X_VERSION_HEADER_NAME, v2.Value)),
		newMessage("4", "order#4"),
	)

	logger.InfoC(ctx, " =========================================================================")
	logger.InfoC(ctx, " poll by active consumer: expect keys 1 (name=active) and 4 (fallback empty)")
	logger.InfoC(ctx, " =========================================================================")
	activeKeys := pollAcceptedKeys(assertions, ctx, consumerActive, 4)
	assertions.Equal([]string{"1", "4"}, activeKeys)

	logger.InfoC(ctx, " =========================================================================")
	logger.InfoC(ctx, " poll by candidate consumer: expect keys 2 (name=candidate) and 3 (fallback v2)")
	logger.InfoC(ctx, " =========================================================================")
	candidateKeys := pollAcceptedKeys(assertions, ctx, consumerCandidate, 4)
	assertions.Equal([]string{"2", "3"}, candidateKeys)
}

// pollAcceptedKeys polls exactly recordCount times (every produced record is delivered as a Poll
// result — declined ones come back with a nil Message) and returns the keys of accepted records.
// A fixed count is used instead of a timeout so it does not race the initial consumer-group join.
func pollAcceptedKeys(assertions *require.Assertions, ctx context.Context, consumer *bgKafka.BgConsumer, recordCount int) []string {
	var keys []string
	for i := 0; i < recordCount; i++ {
		record, err := consumer.Poll(ctx, pollTimeout)
		assertions.NoError(err)
		assertions.NotNil(record)
		if record.Message != nil {
			keys = append(keys, string(record.Message.Key()))
		}
	}
	// no further records must be available
	_, err := consumer.Poll(ctx, noRecordTimeout)
	assertions.ErrorIs(err, context.DeadlineExceeded)
	return keys
}

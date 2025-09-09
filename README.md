[![Go build](https://github.com/Netcracker/qubership-core-lib-go-bg-kafka/actions/workflows/go-build.yml/badge.svg)](https://github.com/Netcracker/qubership-core-lib-go-bg-kafka/actions/workflows/go-build.yml)
[![Coverage](https://sonarcloud.io/api/project_badges/measure?metric=coverage&project=Netcracker_qubership-core-lib-go-bg-kafka)](https://sonarcloud.io/summary/overall?id=Netcracker_qubership-core-lib-go-bg-kafka)
[![duplicated_lines_density](https://sonarcloud.io/api/project_badges/measure?metric=duplicated_lines_density&project=Netcracker_qubership-core-lib-go-bg-kafka)](https://sonarcloud.io/summary/overall?id=Netcracker_qubership-core-lib-go-bg-kafka)
[![vulnerabilities](https://sonarcloud.io/api/project_badges/measure?metric=vulnerabilities&project=Netcracker_qubership-core-lib-go-bg-kafka)](https://sonarcloud.io/summary/overall?id=Netcracker_qubership-core-lib-go-bg-kafka)
[![bugs](https://sonarcloud.io/api/project_badges/measure?metric=bugs&project=Netcracker_qubership-core-lib-go-bg-kafka)](https://sonarcloud.io/summary/overall?id=Netcracker_qubership-core-lib-go-bg-kafka)
[![code_smells](https://sonarcloud.io/api/project_badges/measure?metric=code_smells&project=Netcracker_qubership-core-lib-go-bg-kafka)](https://sonarcloud.io/summary/overall?id=Netcracker_qubership-core-lib-go-bg-kafka)

# Blue-Green for Kafka clients

<!-- TOC -->
* [Blue-Green for Kafka clients](#blue-green-for-kafka-clients)
  * [Blue Green Version tracking filter](#blue-green-version-tracking-filter)
<!-- TOC -->

This library provides abstract implementation to consume messages from Kafka in Cloud-Core Blue Green scenarios

The library is extended by adapters, refer to the documentation of particular adapter:
* [segmentio adapter](https://github.com/netcracker/qubership-core-lib-go-maas-bg-segmentio)

## Blue Green Version tracking filter
This library provides implementation of Blue Green version filter by [filter.go](filter.go).  
This implementation depends on [blue-green-state-monitor-go](https://github.com/netcracker/qubership-core-lib-go-bg-state-monitor/) library.

Usage example:
```go
import (
    "context"
    "fmt"
    bgState "github.com/netcracker/qubership-core-lib-go-bg-state-monitor/v2"
    bgKafka "github.com/netcracker/qubership-core-lib-go-maas-bg-kafka/v3"
)

func FilterDemo(statePublisher *bgState.ConsulBlueGreenStatePublisher) {
    filter := bgKafka.NewTrackingVersionFilter(context.Background(), statePublisher)
    result = filter.Test("v1")
}

```


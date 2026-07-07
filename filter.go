package blue_green_kafka

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"

	bgMonitor "github.com/netcracker/qubership-core-lib-go-bg-state-monitor/v2"
)

type TrackingVersionFilter struct {
	statePublisher BGStatePublisher
	predicate      *atomic.Pointer[Filter]
}

type BGStatePublisher interface {
	Subscribe(ctx context.Context, callback func(state bgMonitor.BlueGreenState))
	GetState() bgMonitor.BlueGreenState
}

func NewTrackingVersionFilter(ctx context.Context, statePublisher BGStatePublisher) *TrackingVersionFilter {
	instance := TrackingVersionFilter{statePublisher: statePublisher, predicate: &atomic.Pointer[Filter]{}}
	instance.predicate.Store(NewFilter(statePublisher.GetState()))
	statePublisher.Subscribe(ctx, func(state bgMonitor.BlueGreenState) {
		instance.predicate.Store(NewFilter(state))
	})
	return &instance
}

func (f *TrackingVersionFilter) Test(version string) (bool, error) {
	return f.predicate.Load().Test(version)
}

func (f *TrackingVersionFilter) String() string {
	return f.predicate.Load().Presentation
}

// newBGStateFilter holds the common blue-green filter skeleton. Accept everything when there is no
// active sibling; when current is ACTIVE reject only the sibling's value; when current is
// CANDIDATE/LEGACY accept only its own value. The extractor/present/parse/eq params carry the only
// logic that differs between concrete filters.
//
//	extractor - pulls the value to match from a namespace (its version or its state name)
//	present   - renders the target for the Presentation string (logging)
//	parse     - parses the header value into the same type before comparison
//	eq        - equality between the parsed header value and the extracted target
func newBGStateFilter[T any](bgState bgMonitor.BlueGreenState,
	extractor func(bgMonitor.NamespaceVersion) T,
	present func(T) string,
	parse func(string) (T, error),
	eq func(a, b T) bool) *Filter {
	current := bgState.Current
	sibling := bgState.Sibling
	if sibling == nil || sibling.State == bgMonitor.StateIdle {
		return newFilter("true", func(v string) (bool, error) { return true, nil })
	}
	match := func(target T, presentation string, accept func(equal bool) bool) *Filter {
		return newFilter(presentation, func(v string) (bool, error) {
			parsed, err := parse(v)
			if err != nil {
				return false, err
			}
			return accept(eq(parsed, target)), nil
		})
	}
	switch current.State {
	case bgMonitor.StateActive:
		target := extractor(*sibling)
		return match(target, "!"+present(target), func(equal bool) bool { return !equal })
	case bgMonitor.StateCandidate, bgMonitor.StateLegacy:
		target := extractor(current)
		return match(target, present(target), func(equal bool) bool { return equal })
	default:
		return newFilter("false", func(v string) (bool, error) {
			return false, fmt.Errorf("invalid state: %s", current.State)
		})
	}
}

// NewFilter builds a filter over the X-Version header value (the version number, e.g. v2).
func NewFilter(bgState bgMonitor.BlueGreenState) *Filter {
	return newBGStateFilter(bgState,
		func(ns bgMonitor.NamespaceVersion) bgMonitor.Version { return *ns.Version },
		func(version bgMonitor.Version) string { return version.String() },
		func(v string) (bgMonitor.Version, error) {
			parsed, err := bgMonitor.NewVersion(v)
			if err != nil {
				return bgMonitor.Version{}, err
			}
			return *parsed, nil
		},
		func(a, b bgMonitor.Version) bool { return a == b })
}

// NewVersionNameFilter builds a filter over the X-Version-Name header value (the BG state name:
// ACTIVE/CANDIDATE/LEGACY). Mirrors NewFilter but matches by state name instead of version number.
func NewVersionNameFilter(bgState bgMonitor.BlueGreenState) *Filter {
	return newBGStateFilter(bgState,
		func(ns bgMonitor.NamespaceVersion) string { return ns.State.String() },
		func(name string) string { return name },
		func(v string) (string, error) { return v, nil },
		func(a, b string) bool { return strings.EqualFold(a, b) })
}

type Filter struct {
	Presentation string
	Test         func(version string) (bool, error)
}

func newFilter(presentation string, checkFunc func(version string) (bool, error)) *Filter {
	return &Filter{Presentation: presentation, Test: checkFunc}
}

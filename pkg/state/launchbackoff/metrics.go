/*
Copyright The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package launchbackoff

import (
	opmetrics "github.com/awslabs/operatorpkg/metrics"
	"github.com/prometheus/client_golang/prometheus"
	crmetrics "sigs.k8s.io/controller-runtime/pkg/metrics"

	"sigs.k8s.io/karpenter/pkg/metrics"
)

const (
	offeringSubsystem = "offerings"

	// ThrottledReasonConstrained and ThrottledReasonRisky separate the two budgets. A NodePool the
	// aggregate budget has already released can still be throttling probes, so a single count would
	// not distinguish "this pool is being held back" from "this launch had nowhere untried to go".
	ThrottledReasonConstrained = "constrained"
	ThrottledReasonRisky       = "risky"
)

// throttledReason is the `reason` dimension of NodePoolsLaunchThrottledTotal. Declared here rather
// than in pkg/metrics because both values are meaningful only to launch backoff.
var throttledReason = opmetrics.Label{
	Name: metrics.ReasonLabel,
	Help: "Which launch budget declined the nodeclaim.",
	Values: []opmetrics.Value{
		{
			Name: ThrottledReasonConstrained,
			Help: "The nodepool's aggregate launch budget is engaged after a recent insufficient capacity failure.",
		},
		{
			Name: ThrottledReasonRisky,
			Help: "Every offering the nodeclaim could launch into has recent failure history, so it was charged to the risky probe budget.",
		},
	},
}

// offeringDimensions is the capacity pool an insufficient capacity failure is attributed to. All
// three are empty when the cloud provider does not attribute the failure to specific offerings.
var offeringDimensions = []opmetrics.Label{
	metrics.InstanceType,
	metrics.CapacityType,
	metrics.Zone,
}

var (
	// OfferingsLaunchFailuresTotal records where capacity was short, which the existing
	// NodePool-labeled disruption counter cannot express. Left unlabelled when the provider does
	// not attribute the failure to specific pools.
	OfferingsLaunchFailuresTotal = opmetrics.NewPrometheusCounter(
		crmetrics.Registry,
		prometheus.CounterOpts{
			Namespace: metrics.Namespace,
			Subsystem: offeringSubsystem,
			Name:      "launch_failures_total",
			Help:      "Number of launches that failed for insufficient capacity, attributed to the capacity pool that refused. Labeled by instance type, capacity type, and zone, all empty when the cloud provider does not attribute the failure.",
		},
		offeringDimensions,
	)
	// NodePoolsLaunchThrottledTotal counts NodeClaims a launch budget declined. This is what
	// separates "Karpenter is holding back" from "there was nothing to do", which is otherwise
	// indistinguishable from the outside.
	NodePoolsLaunchThrottledTotal = opmetrics.NewPrometheusCounter(
		crmetrics.Registry,
		prometheus.CounterOpts{
			Namespace: metrics.Namespace,
			Subsystem: metrics.NodePoolSubsystem,
			Name:      "launch_throttled_total",
			Help:      "Number of nodeclaims not created because a launch budget declined them after an insufficient capacity failure. Labeled by the owning nodepool and which budget declined.",
		},
		[]opmetrics.Label{metrics.NodePool, throttledReason},
	)

	// The gauges below carry series only for offerings and NodePools the tracker currently holds
	// back. They are published by the launch backoff metrics controller, which rebuilds the whole
	// set each pass so that a recovered offering's series disappears rather than pinning at 0.
	// Deliberately not a duplicate of the provider's own per-offering availability signal: nothing
	// is emitted for an offering that has never failed.

	// OfferingsUnavailable answers "which capacity pools is Karpenter refusing to try", which the
	// counter cannot, since a failure count does not say whether the block is still in force.
	OfferingsUnavailable = opmetrics.NewPrometheusGauge(
		crmetrics.Registry,
		prometheus.GaugeOpts{
			Namespace: metrics.Namespace,
			Subsystem: offeringSubsystem,
			Name:      "unavailable",
			Help:      "Set to 1 while Karpenter is holding an offering out of scheduling after an insufficient capacity failure. Labeled by instance type, capacity type, and zone. No series is emitted for offerings that are currently usable.",
		},
		offeringDimensions,
	)
	// NodePoolsLaunchConstrained is the "is this NodePool being throttled" signal. A released pool
	// can still be throttling risky launches, which is why the throttled counter is by reason.
	NodePoolsLaunchConstrained = opmetrics.NewPrometheusGauge(
		crmetrics.Registry,
		prometheus.GaugeOpts{
			Namespace: metrics.Namespace,
			Subsystem: metrics.NodePoolSubsystem,
			Name:      "launch_constrained",
			Help:      "Set to 1 while a nodepool's aggregate launch budget is engaged after an insufficient capacity failure. Labeled by nodepool. No series is emitted for nodepools launching freely.",
		},
		[]opmetrics.Label{metrics.NodePool},
	)
	// NodePoolsLaunchBurst separates a pool that is recovering from one that keeps failing: the
	// ceiling doubles per success and resets to the floor on failure.
	NodePoolsLaunchBurst = opmetrics.NewPrometheusGauge(
		crmetrics.Registry,
		prometheus.GaugeOpts{
			Namespace: metrics.Namespace,
			Subsystem: metrics.NodePoolSubsystem,
			Name:      "launch_burst",
			Help:      "The number of launches a constrained nodepool is allowed per probe window at its current recovery level. Rises as launches succeed and resets to 1 on failure. Labeled by nodepool. No series is emitted for nodepools launching freely.",
		},
		[]opmetrics.Label{metrics.NodePool},
	)
)

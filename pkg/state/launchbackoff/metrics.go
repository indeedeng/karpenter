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
		[]string{
			metrics.InstanceTypeLabel,
			metrics.CapacityTypeLabel,
			metrics.ZoneLabel,
		},
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
		[]string{
			metrics.NodePoolLabel,
			metrics.ReasonLabel,
		},
	)
)

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
	offeringSubsystem             = "offerings"
	ThrottledReasonOfferingBudget = "offering_budget"
	ThrottledCapacityTypeMixed    = "mixed"
)

func ThrottledCapacityType(available []string) string {
	switch len(available) {
	case 0:
		return ""
	case 1:
		return available[0]
	default:
		return ThrottledCapacityTypeMixed
	}
}

var (
	OfferingsLaunchFailuresTotal = opmetrics.NewPrometheusCounter(
		crmetrics.Registry,
		prometheus.CounterOpts{
			Namespace: metrics.Namespace,
			Subsystem: offeringSubsystem,
			Name:      "launch_failures_total",
			Help:      "Number of launches that failed for insufficient capacity, attributed to instance type, capacity type, and zone.",
		},
		[]string{metrics.InstanceTypeLabel, metrics.CapacityTypeLabel, metrics.ZoneLabel},
	)
	OfferingsLaunchBudget = opmetrics.NewPrometheusGauge(
		crmetrics.Registry,
		prometheus.GaugeOpts{
			Namespace: metrics.Namespace,
			Subsystem: offeringSubsystem,
			Name:      "launch_budget",
			Help:      "Current launch allowance ceiling for an offering recovering from insufficient capacity.",
		},
		[]string{metrics.InstanceTypeLabel, metrics.CapacityTypeLabel, metrics.ZoneLabel},
	)
	OfferingsUnavailable = opmetrics.NewPrometheusGauge(
		crmetrics.Registry,
		prometheus.GaugeOpts{
			Namespace: metrics.Namespace,
			Subsystem: offeringSubsystem,
			Name:      "unavailable",
			Help:      "Set to 1 while an offering has no remaining launch allowance in the current refill window.",
		},
		[]string{metrics.InstanceTypeLabel, metrics.CapacityTypeLabel, metrics.ZoneLabel},
	)
	ActiveOfferings = opmetrics.NewPrometheusGauge(
		crmetrics.Registry,
		prometheus.GaugeOpts{
			Namespace: metrics.Namespace,
			Name:      "launch_backoff_active_offerings",
			Help:      "Number of offerings currently tracked by launch backoff.",
		},
		[]string{},
	)
	NodePoolsLaunchProbesTotal = opmetrics.NewPrometheusCounter(
		crmetrics.Registry,
		prometheus.CounterOpts{
			Namespace: metrics.Namespace,
			Subsystem: metrics.NodePoolSubsystem,
			Name:      "launch_probes_total",
			Help:      "Number of NodeClaims whose reservation debited at least one tracked offering.",
		},
		[]string{metrics.NodePoolLabel, metrics.CapacityTypeLabel},
	)
	NodePoolsLaunchProbeOfferings = opmetrics.NewPrometheusHistogram(
		crmetrics.Registry,
		prometheus.HistogramOpts{
			Namespace: metrics.Namespace,
			Subsystem: metrics.NodePoolSubsystem,
			Name:      "launch_probe_offerings",
			Help:      "Number of tracked offering budgets pessimistically debited by a NodeClaim reservation.",
			Buckets:   prometheus.ExponentialBuckets(1, 2, 10),
		},
		[]string{metrics.CapacityTypeLabel},
	)
	NodePoolsLaunchThrottledTotal = opmetrics.NewPrometheusCounter(
		crmetrics.Registry,
		prometheus.CounterOpts{
			Namespace: metrics.Namespace,
			Subsystem: metrics.NodePoolSubsystem,
			Name:      "launch_throttled_total",
			Help:      "Number of NodeClaims not created because every compatible offering budget was exhausted.",
		},
		[]string{metrics.NodePoolLabel, metrics.ReasonLabel, metrics.CapacityTypeLabel},
	)
)

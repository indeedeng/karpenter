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

package launchbackoff_test

import (
	"context"
	"testing"

	opmetrics "github.com/awslabs/operatorpkg/metrics"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/samber/lo"

	"sigs.k8s.io/karpenter/pkg/apis"
	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	metricslaunchbackoff "sigs.k8s.io/karpenter/pkg/controllers/metrics/launchbackoff"
	"sigs.k8s.io/karpenter/pkg/metrics"
	"sigs.k8s.io/karpenter/pkg/operator/options"
	"sigs.k8s.io/karpenter/pkg/state/launchbackoff"
	"sigs.k8s.io/karpenter/pkg/test"
	. "sigs.k8s.io/karpenter/pkg/test/expectations"
	"sigs.k8s.io/karpenter/pkg/test/v1alpha1"
	. "sigs.k8s.io/karpenter/pkg/utils/testing"
)

var (
	ctx           context.Context
	env           *test.Environment
	controller    *metricslaunchbackoff.Controller
	launchBackoff *launchbackoff.Tracker
)

func TestAPIs(t *testing.T) {
	ctx = TestContextWithLogger(t)
	RegisterFailHandler(Fail)
	RunSpecs(t, "LaunchBackoffMetrics")
}

var _ = BeforeSuite(func() {
	env = test.NewEnvironment(test.WithCRDs(apis.CRDs...), test.WithCRDs(v1alpha1.CRDs...))
})

var _ = AfterSuite(func() {
	Expect(env.Stop()).To(Succeed(), "Failed to stop environment")
})

var _ = AfterEach(func() {
	ExpectCleanedUp(ctx, env.Client)
})

var _ = Describe("Launch Backoff Metrics", func() {
	var nodePool *v1.NodePool
	offering := cloudprovider.OfferingKey{InstanceType: "large", CapacityType: v1.CapacityTypeSpot, Zone: "test-zone-1a"}

	// unavailableGauge reports whether a series exists for the offering, which is the assertion that
	// matters: a recovered offering must stop being reported rather than report 0.
	unavailableGauge := func(key cloudprovider.OfferingKey) (float64, bool) {
		m, ok := FindMetricWithLabelValues(ExpectMetricName(launchbackoff.OfferingsUnavailable.(*opmetrics.PrometheusGauge)), map[string]string{
			metrics.InstanceTypeLabel: key.InstanceType,
			metrics.CapacityTypeLabel: key.CapacityType,
			metrics.ZoneLabel:         key.Zone,
		})
		if !ok {
			return 0, false
		}
		return lo.FromPtr(m.Gauge.Value), true
	}

	constrainedGauge := func(name string) (float64, bool) {
		m, ok := FindMetricWithLabelValues(ExpectMetricName(launchbackoff.NodePoolsLaunchConstrained.(*opmetrics.PrometheusGauge)), map[string]string{
			metrics.NodePoolLabel: name,
		})
		if !ok {
			return 0, false
		}
		return lo.FromPtr(m.Gauge.Value), true
	}

	BeforeEach(func() {
		ctx = options.ToContext(ctx, test.Options(test.OptionsFields{
			FeatureGates: test.FeatureGates{LaunchBackoff: lo.ToPtr(true)},
		}))
		launchBackoff = launchbackoff.NewTracker(env.Clock)
		controller = metricslaunchbackoff.NewController(env.Client, launchBackoff)
		nodePool = test.NodePool()
		ExpectApplied(ctx, env.Client, nodePool)
		nodePool = ExpectExists(ctx, env.Client, nodePool)
	})

	Context("offerings", func() {
		It("should emit nothing for an offering that has never failed", func() {
			ExpectSingletonReconciled(ctx, controller)

			_, found := unavailableGauge(offering)
			Expect(found).To(BeFalse())
		})
		It("should report a backed-off offering as unavailable", func() {
			launchBackoff.Fail(ctx, offering)

			ExpectSingletonReconciled(ctx, controller)

			value, found := unavailableGauge(offering)
			Expect(found).To(BeTrue())
			Expect(value).To(Equal(float64(1)))
		})
		It("should drop the series once the window elapses", func() {
			// Not a 0: an offering leaving its window generates no event, so a series that lingered
			// would need something to clear it and there is nothing to fire.
			launchBackoff.Fail(ctx, offering)
			ExpectSingletonReconciled(ctx, controller)
			env.Clock.Step(launchbackoff.BaseDelay * 2)

			ExpectSingletonReconciled(ctx, controller)

			_, found := unavailableGauge(offering)
			Expect(found).To(BeFalse())
		})
		It("should drop the series once a launch succeeds", func() {
			launchBackoff.Fail(ctx, offering)
			ExpectSingletonReconciled(ctx, controller)
			launchBackoff.Succeed(ctx, offering)

			ExpectSingletonReconciled(ctx, controller)

			_, found := unavailableGauge(offering)
			Expect(found).To(BeFalse())
		})
	})

	Context("nodepools", func() {
		It("should emit nothing for a nodepool launching freely", func() {
			ExpectSingletonReconciled(ctx, controller)

			_, found := constrainedGauge(nodePool.Name)
			Expect(found).To(BeFalse())
		})
		It("should report a constrained nodepool by name", func() {
			launchBackoff.FailPool(ctx, nodePool.UID)

			ExpectSingletonReconciled(ctx, controller)

			value, found := constrainedGauge(nodePool.Name)
			Expect(found).To(BeTrue())
			Expect(value).To(Equal(float64(1)))
		})
		It("should report the burst ceiling at the recovery floor", func() {
			launchBackoff.FailPool(ctx, nodePool.UID)

			ExpectSingletonReconciled(ctx, controller)

			ExpectMetricGaugeValue(launchbackoff.NodePoolsLaunchBurst, 1, map[string]string{metrics.NodePoolLabel: nodePool.Name})
		})
		It("should raise the burst ceiling as launches succeed", func() {
			// The ceiling, not the remaining allowance. Consumption would sawtooth and hide the ramp.
			launchBackoff.FailPool(ctx, nodePool.UID)
			launchBackoff.SucceedPool(ctx, nodePool.UID)

			ExpectSingletonReconciled(ctx, controller)

			ExpectMetricGaugeValue(launchbackoff.NodePoolsLaunchBurst, 2, map[string]string{metrics.NodePoolLabel: nodePool.Name})
		})
		It("should drop the series once the nodepool is released", func() {
			launchBackoff.FailPool(ctx, nodePool.UID)
			ExpectSingletonReconciled(ctx, controller)
			for range 4 {
				launchBackoff.SucceedPool(ctx, nodePool.UID)
			}

			ExpectSingletonReconciled(ctx, controller)

			_, found := constrainedGauge(nodePool.Name)
			Expect(found).To(BeFalse())
		})
		It("should skip a constrained nodepool whose object is gone", func() {
			// The tracker keys by UID, which is meaningless on a dashboard. Nothing is emitted rather
			// than a series labeled with a UID.
			launchBackoff.FailPool(ctx, nodePool.UID)
			ExpectDeleted(ctx, env.Client, nodePool)

			ExpectSingletonReconciled(ctx, controller)

			_, found := constrainedGauge(nodePool.Name)
			Expect(found).To(BeFalse())
		})
	})

	It("should clear every series when the gate is turned off", func() {
		// State outlives a gate flip, so without this a rolled-back cluster would keep graphing as
		// throttled while nothing is actually held back.
		launchBackoff.Fail(ctx, offering)
		launchBackoff.FailPool(ctx, nodePool.UID)
		ExpectSingletonReconciled(ctx, controller)
		ctx = options.ToContext(ctx, test.Options())

		ExpectSingletonReconciled(ctx, controller)

		_, offeringFound := unavailableGauge(offering)
		Expect(offeringFound).To(BeFalse())
		_, poolFound := constrainedGauge(nodePool.Name)
		Expect(poolFound).To(BeFalse())
	})
})

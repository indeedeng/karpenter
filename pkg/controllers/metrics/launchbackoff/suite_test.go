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
	env.Clock.Step(launchbackoff.EntryTTL)
	launchBackoff.Cleanup()
	ExpectSingletonReconciled(ctx, controller)
	ExpectCleanedUp(ctx, env.Client)
})

var _ = Describe("Launch Backoff Metrics", func() {
	var (
		offering  cloudprovider.OfferingKey
		alternate cloudprovider.OfferingKey
	)

	offeringGauge := func(gauge *opmetrics.PrometheusGauge, key cloudprovider.OfferingKey) (float64, bool) {
		m, ok := FindMetricWithLabelValues(ExpectMetricName(gauge), map[string]string{
			metrics.InstanceTypeLabel: key.InstanceType,
			metrics.CapacityTypeLabel: key.CapacityType,
			metrics.ZoneLabel:         key.Zone,
		})
		if !ok {
			return 0, false
		}
		return lo.FromPtr(m.Gauge.Value), true
	}

	activeOfferings := func() float64 {
		m, ok := FindMetricWithLabelValues(ExpectMetricName(launchbackoff.ActiveOfferings.(*opmetrics.PrometheusGauge)), map[string]string{})
		Expect(ok).To(BeTrue())
		return lo.FromPtr(m.Gauge.Value)
	}

	BeforeEach(func() {
		ctx = options.ToContext(ctx, test.Options(test.OptionsFields{
			FeatureGates: test.FeatureGates{LaunchBackoff: lo.ToPtr(true)},
		}))
		launchBackoff = launchbackoff.NewTracker(env.Clock)
		controller = metricslaunchbackoff.NewController(env.Client, launchBackoff)
		offering = cloudprovider.OfferingKey{
			InstanceType: "large",
			CapacityType: v1.CapacityTypeSpot,
			Zone:         "test-zone-1a",
		}
		alternate = cloudprovider.OfferingKey{
			InstanceType: "small",
			CapacityType: v1.CapacityTypeOnDemand,
			Zone:         "test-zone-1b",
		}
	})

	It("should report zero active offerings and no per-offering series without history", func() {
		ExpectSingletonReconciled(ctx, controller)

		_, budgetFound := offeringGauge(launchbackoff.OfferingsLaunchBudget.(*opmetrics.PrometheusGauge), offering)
		_, unavailableFound := offeringGauge(launchbackoff.OfferingsUnavailable.(*opmetrics.PrometheusGauge), offering)
		Expect(budgetFound).To(BeFalse())
		Expect(unavailableFound).To(BeFalse())
		Expect(activeOfferings()).To(BeZero())
	})
	It("should report launch budget, unavailability, and active offering count", func() {
		launchBackoff.Fail(ctx, "failure", offering)

		ExpectSingletonReconciled(ctx, controller)

		budget, budgetFound := offeringGauge(launchbackoff.OfferingsLaunchBudget.(*opmetrics.PrometheusGauge), offering)
		unavailable, unavailableFound := offeringGauge(launchbackoff.OfferingsUnavailable.(*opmetrics.PrometheusGauge), offering)
		Expect(budgetFound).To(BeTrue())
		Expect(budget).To(Equal(float64(1)))
		Expect(unavailableFound).To(BeTrue())
		Expect(unavailable).To(Equal(float64(1)))
		Expect(activeOfferings()).To(Equal(float64(1)))
	})
	It("should retain the budget but remove unavailability after the refill window", func() {
		launchBackoff.Fail(ctx, "failure", offering)
		ExpectSingletonReconciled(ctx, controller)
		env.Clock.Step(launchbackoff.ProbeInterval)

		ExpectSingletonReconciled(ctx, controller)

		budget, budgetFound := offeringGauge(launchbackoff.OfferingsLaunchBudget.(*opmetrics.PrometheusGauge), offering)
		_, unavailableFound := offeringGauge(launchbackoff.OfferingsUnavailable.(*opmetrics.PrometheusGauge), offering)
		Expect(budgetFound).To(BeTrue())
		Expect(budget).To(Equal(float64(1)))
		Expect(unavailableFound).To(BeFalse())
		Expect(activeOfferings()).To(Equal(float64(1)))
	})
	It("should report a successful reservation ramp and refund", func() {
		launchBackoff.Fail(ctx, "failure-offering", offering)
		launchBackoff.Fail(ctx, "failure-alternate", alternate)
		env.Clock.Step(launchbackoff.ProbeInterval)
		result := launchBackoff.Reserve(ctx, "reservation", []cloudprovider.OfferingKey{offering, alternate})
		Expect(result.Admitted).To(BeTrue())
		Expect(result.DebitedOfferings).To(Equal(2))
		launchBackoff.Succeed(ctx, "reservation", offering)

		ExpectSingletonReconciled(ctx, controller)

		budget, budgetFound := offeringGauge(launchbackoff.OfferingsLaunchBudget.(*opmetrics.PrometheusGauge), offering)
		_, unavailableFound := offeringGauge(launchbackoff.OfferingsUnavailable.(*opmetrics.PrometheusGauge), offering)
		Expect(budgetFound).To(BeTrue())
		Expect(budget).To(Equal(float64(2)))
		Expect(unavailableFound).To(BeTrue())

		alternateBudget, alternateBudgetFound := offeringGauge(launchbackoff.OfferingsLaunchBudget.(*opmetrics.PrometheusGauge), alternate)
		_, alternateUnavailableFound := offeringGauge(launchbackoff.OfferingsUnavailable.(*opmetrics.PrometheusGauge), alternate)
		Expect(alternateBudgetFound).To(BeTrue())
		Expect(alternateBudget).To(Equal(float64(1)))
		Expect(alternateUnavailableFound).To(BeFalse())
		Expect(activeOfferings()).To(Equal(float64(2)))
	})
	It("should remove expired offering series and decrement the active count", func() {
		launchBackoff.Fail(ctx, "failure", offering)
		ExpectSingletonReconciled(ctx, controller)
		env.Clock.Step(launchbackoff.EntryTTL)
		launchBackoff.Cleanup()

		ExpectSingletonReconciled(ctx, controller)

		_, budgetFound := offeringGauge(launchbackoff.OfferingsLaunchBudget.(*opmetrics.PrometheusGauge), offering)
		_, unavailableFound := offeringGauge(launchbackoff.OfferingsUnavailable.(*opmetrics.PrometheusGauge), offering)
		Expect(budgetFound).To(BeFalse())
		Expect(unavailableFound).To(BeFalse())
		Expect(activeOfferings()).To(BeZero())
	})
	It("should release a bound reservation only after its NodeClaim no longer exists", func() {
		launchBackoff.Fail(ctx, "failure", offering)
		env.Clock.Step(launchbackoff.ProbeInterval)
		Expect(launchBackoff.Reserve(ctx, "reservation", []cloudprovider.OfferingKey{offering}).Admitted).To(BeTrue())

		nodeClaim := test.NodeClaim()
		ExpectApplied(ctx, env.Client, nodeClaim)
		nodeClaim = ExpectExists(ctx, env.Client, nodeClaim)
		launchBackoff.Bind("reservation", nodeClaim.UID)

		ExpectSingletonReconciled(ctx, controller)
		Expect(launchBackoff.Reserve(ctx, "competing", []cloudprovider.OfferingKey{offering}).Admitted).To(BeFalse())

		ExpectDeleted(ctx, env.Client, nodeClaim)
		ExpectSingletonReconciled(ctx, controller)
		Expect(launchBackoff.Reserve(ctx, "competing", []cloudprovider.OfferingKey{offering}).Admitted).To(BeTrue())
	})
})

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

// Reclaiming a NodePool's budget entry has no effect a caller can observe — that is the point
// of decay — so these specs read the maps directly rather than motivating an accessor that
// only tests would use.

import (
	"context"
	"time"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"k8s.io/apimachinery/pkg/types"
	clocktesting "k8s.io/utils/clock/testing"

	"sigs.k8s.io/karpenter/pkg/operator/options"
)

var _ = Describe("Pool entry decay", func() {
	var ctx context.Context
	var clk *clocktesting.FakeClock
	var t *Tracker
	uid := types.UID("nodepool-a")

	BeforeEach(func() {
		ctx = options.ToContext(context.Background(), &options.Options{
			FeatureGates: options.FeatureGates{LaunchBackoff: true},
		})
		clk = clocktesting.NewFakeClock(time.Now())
		t = NewTracker(clk)
	})

	It("should collect a released NodePool once it has gone quiet", func() {
		t.FailPool(ctx, uid)
		for range 4 {
			t.SucceedPool(ctx, uid)
		}
		Expect(t.pools).To(HaveLen(1))

		t.GC()
		Expect(t.pools).To(HaveLen(1), "the aggregate window is still live")

		clk.Step(ProbeInterval + MaxDelay)
		t.GC()

		Expect(t.pools).To(BeEmpty())
	})
	It("should retain a NodePool with a live risky window", func() {
		Expect(t.Admit(ctx, uid, true)).To(BeTrue())

		t.GC()

		Expect(t.pools).To(HaveLen(1))
	})
	It("should collect a NodePool whose risky window has been quiet for MaxDelay", func() {
		Expect(t.Admit(ctx, uid, true)).To(BeTrue())
		clk.Step(ProbeInterval + MaxDelay)

		t.GC()

		Expect(t.pools).To(BeEmpty())
	})
	It("should not let ordinary admissions keep a recovered NodePool alive", func() {
		Expect(t.Admit(ctx, uid, true)).To(BeTrue())
		clk.Step(ProbeInterval + MaxDelay)

		// A busy healthy NodePool admits constantly. If admission refreshed the risky window
		// unconditionally, the entry would never age out.
		for range 100 {
			Expect(t.Admit(ctx, uid, false)).To(BeTrue())
		}
		t.GC()

		Expect(t.pools).To(BeEmpty())
	})
	It("should not create an entry for an ordinary admission on a healthy NodePool", func() {
		Expect(t.Admit(ctx, uid, false)).To(BeTrue())

		Expect(t.pools).To(BeEmpty())
	})
})

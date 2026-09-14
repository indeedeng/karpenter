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

package provisioning

import (
	"errors"
	"testing"
	"time"

	"github.com/awslabs/operatorpkg/singleton"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clock "k8s.io/utils/clock/testing"

	scheduler "sigs.k8s.io/karpenter/pkg/controllers/provisioning/scheduling"
	"sigs.k8s.io/karpenter/pkg/state/launchbackoff"
)

func TestRequeueForSchedulingResults(t *testing.T) {
	now := time.Now()
	provisioner := &Provisioner{clock: clock.NewFakeClock(now)}
	pod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "pod"}}
	blockedPod := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: "blocked-pod"}}

	t.Run("bounds an all-offering-blocked retry", func(t *testing.T) {
		result := provisioner.requeueForSchedulingResults(scheduler.Results{
			PodErrors: map[*corev1.Pod]error{
				pod: scheduler.NewOfferingsUnavailableError(errors.New("all offerings unavailable")),
			},
		}, ReservationResults{})
		if result.RequeueAfter != launchbackoff.ProbeInterval {
			t.Fatalf("expected %s, got %s", launchbackoff.ProbeInterval, result.RequeueAfter)
		}
	})

	t.Run("uses an omitted claim's earlier refill", func(t *testing.T) {
		result := provisioner.requeueForSchedulingResults(scheduler.Results{}, ReservationResults{
			Omitted:      []*scheduler.NodeClaim{{}},
			NextEligible: now.Add(10 * time.Second),
		})
		if result.RequeueAfter != 10*time.Second {
			t.Fatalf("expected 10s, got %s", result.RequeueAfter)
		}
	})

	t.Run("sleeps until refill when all pending work is offerings-blocked", func(t *testing.T) {
		result := provisioner.requeueForSchedulingResults(scheduler.Results{
			PodErrors: map[*corev1.Pod]error{
				pod: scheduler.NewOfferingsUnavailableError(errors.New("all offerings unavailable")),
			},
		}, ReservationResults{
			Omitted:      []*scheduler.NodeClaim{{}},
			NextEligible: now.Add(10 * time.Second),
		})
		if result.RequeueAfter != 10*time.Second {
			t.Fatalf("expected 10s, got %s", result.RequeueAfter)
		}
	})

	t.Run("preserves immediate retry for other failures", func(t *testing.T) {
		result := provisioner.requeueForSchedulingResults(scheduler.Results{
			PodErrors: map[*corev1.Pod]error{pod: errors.New("requirements do not match")},
		}, ReservationResults{})
		if result.RequeueAfter != singleton.RequeueImmediately {
			t.Fatalf("expected immediate retry, got %s", result.RequeueAfter)
		}
	})

	t.Run("preserves immediate retry when an oversized pod accompanies an omitted claim", func(t *testing.T) {
		result := provisioner.requeueForSchedulingResults(scheduler.Results{
			PodErrors: map[*corev1.Pod]error{pod: errors.New("no instance type has enough resources")},
		}, ReservationResults{
			Omitted:      []*scheduler.NodeClaim{{}},
			NextEligible: now.Add(10 * time.Second),
		})
		if result.RequeueAfter != singleton.RequeueImmediately {
			t.Fatalf("expected immediate retry, got %s", result.RequeueAfter)
		}
	})

	t.Run("preserves immediate retry when an affinity failure accompanies an offerings-blocked pod", func(t *testing.T) {
		result := provisioner.requeueForSchedulingResults(scheduler.Results{
			PodErrors: map[*corev1.Pod]error{
				pod:        errors.New("required pod affinity cannot be satisfied"),
				blockedPod: scheduler.NewOfferingsUnavailableError(errors.New("all offerings unavailable")),
			},
		}, ReservationResults{})
		if result.RequeueAfter != singleton.RequeueImmediately {
			t.Fatalf("expected immediate retry, got %s", result.RequeueAfter)
		}
	})
}

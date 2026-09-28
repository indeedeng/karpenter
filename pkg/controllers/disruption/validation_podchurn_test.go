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

package disruption

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"

	"sigs.k8s.io/karpenter/pkg/controllers/provisioning/scheduling"
)

func TestNodeClaimsHostingCandidatePods(t *testing.T) {
	pod := func(uid string) *corev1.Pod {
		return &corev1.Pod{ObjectMeta: metav1.ObjectMeta{UID: types.UID(uid)}}
	}
	candidates := []*Candidate{
		{reschedulablePods: []*corev1.Pod{pod("candidate-a")}},
		{reschedulablePods: []*corev1.Pod{pod("candidate-b")}},
	}
	replacement := &scheduling.NodeClaim{Pods: []*corev1.Pod{pod("candidate-a"), pod("pending-1")}}
	pendingOnly := &scheduling.NodeClaim{Pods: []*corev1.Pod{pod("pending-2")}}
	deletingNodeOnly := &scheduling.NodeClaim{Pods: []*corev1.Pod{pod("deleting-node-pod")}}
	secondReplacement := &scheduling.NodeClaim{Pods: []*corev1.Pod{pod("candidate-b")}}

	got := nodeClaimsHostingCandidatePods([]*scheduling.NodeClaim{replacement, pendingOnly, deletingNodeOnly, secondReplacement}, candidates)

	if len(got) != 2 || got[0] != replacement || got[1] != secondReplacement {
		t.Fatalf("expected only NodeClaims hosting candidate pods, got %d NodeClaims", len(got))
	}
	if got := nodeClaimsHostingCandidatePods([]*scheduling.NodeClaim{pendingOnly}, candidates); len(got) != 0 {
		t.Fatalf("expected no NodeClaims when none host candidate pods, got %d", len(got))
	}
}

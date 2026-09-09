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

package state

import (
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/utils/clock"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
)

func TestIsNodeActiveUsesLiveClusterState(t *testing.T) {
	cluster := NewCluster(clock.RealClock{}, nil, nil)
	nodeClaim := &v1.NodeClaim{
		ObjectMeta: metav1.ObjectMeta{
			Name:   "candidate",
			Labels: map[string]string{v1.NodePoolLabelKey: "default"},
		},
		Status: v1.NodeClaimStatus{ProviderID: "provider-id"},
	}
	cluster.UpdateNodeClaim(nodeClaim)
	snapshot := cluster.DeepCopyNodes()

	if !cluster.IsNodeActive(nodeClaim.Status.ProviderID) {
		t.Fatal("expected NodeClaim to be active")
	}
	cluster.MarkForDeletion(nodeClaim.Status.ProviderID)
	if cluster.IsNodeActive(nodeClaim.Status.ProviderID) {
		t.Fatal("expected live cluster state to report the NodeClaim deleting")
	}
	if len(snapshot.Active()) != 1 {
		t.Fatal("expected cached snapshot to remain active and demonstrate the race")
	}
	if cluster.IsNodeActive("missing") {
		t.Fatal("expected a missing NodeClaim to be inactive")
	}
}

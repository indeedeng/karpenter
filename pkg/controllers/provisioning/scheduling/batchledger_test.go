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

package scheduling

import (
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/util/sets"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	karpenterscheduling "sigs.k8s.io/karpenter/pkg/scheduling"
)

func TestBatchLedgerSeedsAcceptedReplacementConsumption(t *testing.T) {
	ledger := NewBatchLedger()
	first := &Scheduler{
		reservationManager: NewReservationManager(nil),
	}
	results := Results{NewNodeClaims: []*NodeClaim{{
		NodeClaimTemplate: NodeClaimTemplate{
			NodePoolName: "default",
			InstanceTypeOptions: []*cloudprovider.InstanceType{{
				Capacity: corev1.ResourceList{
					corev1.ResourceCPU:  resource.MustParse("4"),
					corev1.ResourcePods: resource.MustParse("10"),
				},
			}},
		},
	}}}
	ledger.Commit(first, results, nil, nil)

	second := &Scheduler{
		remainingResources: map[string]corev1.ResourceList{
			"default": {
				corev1.ResourceCPU:  resource.MustParse("10"),
				corev1.ResourcePods: resource.MustParse("20"),
			},
		},
	}
	ledger.seedScheduler(second)

	if actual := second.remainingResources["default"][corev1.ResourceCPU]; actual.Cmp(resource.MustParse("6")) != 0 {
		t.Fatalf("expected 6 CPU remaining, got %s", actual.String())
	}
	if actual := second.remainingResources["default"][corev1.ResourcePods]; actual.Cmp(resource.MustParse("10")) != 0 {
		t.Fatalf("expected 10 pods remaining, got %s", actual.String())
	}
}

func TestBatchLedgerReservationManagerCloneAndCommitIsolation(t *testing.T) {
	offering := &cloudprovider.Offering{
		ReservationCapacity: 1,
		Requirements: karpenterscheduling.NewLabelRequirements(map[string]string{
			v1.CapacityTypeLabelKey:          v1.CapacityTypeReserved,
			corev1.LabelTopologyZone:         "test-zone",
			cloudprovider.ReservationIDLabel: "test-reservation",
		}),
	}
	instanceTypes := map[string][]*cloudprovider.InstanceType{
		"default": {{
			Offerings: cloudprovider.Offerings{offering},
		}},
	}
	ledger := NewBatchLedger()

	rejected := ledger.reservationManagerFor(instanceTypes)
	rejected.Reserve("rejected-before-commit", offering)
	if fresh := ledger.reservationManagerFor(instanceTypes); fresh.RemainingCapacity(offering) != 1 {
		t.Fatalf("expected an uncommitted reservation to remain isolated, got %d capacity", fresh.RemainingCapacity(offering))
	}

	accepted := ledger.reservationManagerFor(instanceTypes)
	accepted.Reserve("accepted", offering)
	ledger.Commit(&Scheduler{reservationManager: accepted}, Results{}, nil, nil)

	accepted.Release("accepted", offering)
	accepted.Reserve("rejected-after-commit", offering)
	replay := ledger.reservationManagerFor(instanceTypes)
	if replay.RemainingCapacity(offering) != 0 {
		t.Fatalf("expected the committed reservation to exhaust capacity, got %d", replay.RemainingCapacity(offering))
	}
	if !replay.HasReservation("accepted", offering) {
		t.Fatal("expected the accepted reservation to be replayed")
	}
	if replay.HasReservation("rejected-after-commit", offering) {
		t.Fatal("expected post-commit scheduler mutations to remain isolated")
	}
	if replay.CanReserve("fallback", offering) {
		t.Fatal("expected later simulations to fall back after reservation exhaustion")
	}

	replay.Release("accepted", offering)
	replay.Reserve("rejected-replay", offering)
	fresh := ledger.reservationManagerFor(instanceTypes)
	if fresh.RemainingCapacity(offering) != 0 {
		t.Fatalf("expected replay mutations to remain isolated, got %d capacity", fresh.RemainingCapacity(offering))
	}
	if fresh.HasReservation("rejected-replay", offering) {
		t.Fatal("expected mutations to a reservation manager clone to remain isolated")
	}
}

func TestBatchLedgerReplaysAcceptedTopologyPlacementsWithoutRejectedState(t *testing.T) {
	acceptedPod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default",
			Name:      "accepted",
			Labels:    map[string]string{"app": "test"},
		},
	}
	ledger := NewBatchLedger()
	ledger.Commit(&Scheduler{}, Results{NewNodeClaims: []*NodeClaim{{
		NodeClaimTemplate: NodeClaimTemplate{
			NodePoolName: "default",
			Requirements: karpenterscheduling.NewRequirements(),
		},
		Pods:     []*corev1.Pod{acceptedPod},
		hostname: "accepted-host",
	}}}, nil, nil)

	acceptedPod.Labels["app"] = "mutated-after-commit"
	rejectedTopology, rejectedGroup := newBatchLedgerTestTopology()
	ledger.seedScheduler(&Scheduler{
		topology:           rejectedTopology,
		remainingResources: map[string]corev1.ResourceList{},
	})
	if actual := rejectedGroup.domains["accepted-host"]; actual != 1 {
		t.Fatalf("expected accepted placement to be replayed once, got %d", actual)
	}

	rejectedPod := acceptedPod.DeepCopy()
	rejectedPod.Labels["app"] = "test"
	rejectedTopology.Register(corev1.LabelHostname, "rejected-host")
	rejectedTopology.Record(rejectedPod, nil, karpenterscheduling.NewLabelRequirements(map[string]string{
		corev1.LabelHostname: "rejected-host",
	}))
	if actual := rejectedGroup.domains["rejected-host"]; actual != 1 {
		t.Fatalf("expected rejected simulation to record its placement once, got %d", actual)
	}

	freshTopology, freshGroup := newBatchLedgerTestTopology()
	ledger.seedScheduler(&Scheduler{
		topology:           freshTopology,
		remainingResources: map[string]corev1.ResourceList{},
	})
	if actual := freshGroup.domains["accepted-host"]; actual != 1 {
		t.Fatalf("expected accepted placement to be replayed once, got %d", actual)
	}
	if _, ok := freshGroup.domains["rejected-host"]; ok {
		t.Fatal("expected rejected topology state to remain isolated")
	}
}

func newBatchLedgerTestTopology() (*Topology, *TopologyGroup) {
	pod := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Namespace: "default",
			Labels:    map[string]string{"app": "test"},
		},
	}
	group := NewTopologyGroup(
		TopologyTypeSpread,
		corev1.LabelHostname,
		pod,
		sets.New("default"),
		&metav1.LabelSelector{MatchLabels: map[string]string{"app": "test"}},
		1,
		nil,
		nil,
		nil,
		NewTopologyDomainGroup(),
	)
	return &Topology{
		topologyGroups:        map[uint64]*TopologyGroup{group.Hash(): group},
		inverseTopologyGroups: map[uint64]*TopologyGroup{},
	}, group
}

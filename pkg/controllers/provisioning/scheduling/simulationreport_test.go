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

package scheduling_test

import (
	"context"
	"testing"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	"k8s.io/utils/clock"
	fakeclient "sigs.k8s.io/controller-runtime/pkg/client/fake"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	cloudproviderfake "sigs.k8s.io/karpenter/pkg/cloudprovider/fake"
	"sigs.k8s.io/karpenter/pkg/controllers/provisioning/scheduling"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/events"
	"sigs.k8s.io/karpenter/pkg/operator/injection"
	"sigs.k8s.io/karpenter/pkg/operator/options"
	"sigs.k8s.io/karpenter/pkg/test"
)

func TestSimulationReportTracksSchedulerWork(t *testing.T) {
	ctx := options.ToContext(injection.WithControllerName(context.Background(), "simulation-report-test"), test.Options())
	kubeClient := fakeclient.NewFakeClient()
	cloudProvider := cloudproviderfake.NewCloudProvider()
	instanceTypes := cloudproviderfake.InstanceTypes(1)
	cloudProvider.InstanceTypes = instanceTypes
	nodePool := test.NodePool(v1.NodePool{})
	pod := test.Pod(test.PodOptions{})
	pod.UID = types.UID("simulation-report-pod")
	pods := []*corev1.Pod{pod}
	cluster := state.NewCluster(&clock.RealClock{}, kubeClient, cloudProvider)
	report := scheduling.NewSimulationReport()
	reporting := scheduling.WithSimulationReport(report)

	topology, err := scheduling.NewTopology(
		ctx,
		kubeClient,
		cluster,
		nil,
		[]*v1.NodePool{nodePool},
		map[string][]*cloudprovider.InstanceType{nodePool.Name: instanceTypes},
		pods,
		reporting,
	)
	if err != nil {
		t.Fatalf("creating topology, %v", err)
	}
	scheduler := scheduling.NewScheduler(
		ctx,
		kubeClient,
		[]*v1.NodePool{nodePool},
		cluster,
		nil,
		topology,
		map[string][]*cloudprovider.InstanceType{nodePool.Name: instanceTypes},
		nil,
		events.NewRecorder(&record.FakeRecorder{}),
		&clock.RealClock{},
		nil,
		nil,
		reporting,
	)
	results, err := scheduler.Solve(ctx, pods)
	if err != nil {
		t.Fatalf("solving, %v", err)
	}

	stats := report.Stats()
	if stats.Inputs.Pods != 1 || stats.Inputs.NodePools != 1 ||
		stats.Inputs.NodePoolTemplates != 1 || stats.Inputs.InstanceTypes != 1 {
		t.Fatalf("unexpected input stats %#v", stats.Inputs)
	}
	if stats.Work.PodAttempts != 1 || stats.Work.NodePoolTemplateChecks != 1 {
		t.Fatalf("unexpected work stats %#v", stats.Work)
	}
	if stats.Results.NewNodeClaims != 1 || stats.Results.PodsOnNewNodeClaims != 1 ||
		stats.Results.PodErrors != 0 || len(results.PodErrors) != 0 {
		t.Fatalf("unexpected result stats %#v", stats.Results)
	}
	if stats.Phases.TopologyBuild <= 0 || stats.Phases.SchedulerBuild <= 0 ||
		stats.Phases.PodDataPreparation <= 0 || stats.Phases.Scheduling <= 0 ||
		stats.Phases.ResultFinalization <= 0 || stats.Phases.Solve <= 0 {
		t.Fatalf("expected positive monotonic phase durations, got %#v", stats.Phases)
	}
}

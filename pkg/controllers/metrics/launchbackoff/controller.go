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
	"context"
	"fmt"
	"time"

	"github.com/awslabs/operatorpkg/reconciler"
	"github.com/awslabs/operatorpkg/singleton"
	"k8s.io/apimachinery/pkg/types"
	controllerruntime "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/metrics"
	"sigs.k8s.io/karpenter/pkg/operator/injection"
	"sigs.k8s.io/karpenter/pkg/operator/options"
	"sigs.k8s.io/karpenter/pkg/state/launchbackoff"
)

// pollInterval bounds how stale a gauge can be. Backoff windows start at 30s, so a shorter
// interval would mostly resample unchanged state and a much longer one could miss a whole window.
const pollInterval = 5 * time.Second

// Controller publishes the launch backoff state gauges.
//
// A polling singleton rather than gauge writes at the tracker's mutation sites, because the state
// these gauges describe is a clock comparison: an offering leaves its backoff window with no
// corresponding event, so nothing would fire to clear the series. Polling also lets the whole
// series set be rebuilt each pass, which is what makes a recovered offering's series disappear
// instead of pinning at 0 forever.
type Controller struct {
	kubeClient    client.Client
	launchBackoff *launchbackoff.Tracker
	metricStore   *metrics.Store
}

func NewController(kubeClient client.Client, launchBackoff *launchbackoff.Tracker) *Controller {
	return &Controller{
		kubeClient:    kubeClient,
		launchBackoff: launchBackoff,
		metricStore:   metrics.NewStore(),
	}
}

func (c *Controller) Name() string {
	return "metrics.launchbackoff"
}

func (c *Controller) Reconcile(ctx context.Context) (reconciler.Result, error) {
	ctx = injection.WithControllerName(ctx, c.Name())

	// Tracker state outlives a gate flip, so a cluster that rolled the feature back would keep
	// graphing as throttled while nothing is actually held back. Clearing rather than returning
	// early, because the series already published have to go somewhere.
	if !options.FromContext(ctx).FeatureGates.LaunchBackoff {
		c.metricStore.ReplaceAll(map[string][]*metrics.StoreMetric{})
		return reconciler.Result{RequeueAfter: pollInterval}, nil
	}

	nodePoolNames, err := c.nodePoolNames(ctx)
	if err != nil {
		return reconciler.Result{}, fmt.Errorf("listing nodepools, %w", err)
	}

	metricsMap := map[string][]*metrics.StoreMetric{}
	for _, key := range c.launchBackoff.UnavailableOfferings() {
		metricsMap[fmt.Sprintf("offering/%s/%s/%s", key.InstanceType, key.CapacityType, key.Zone)] = []*metrics.StoreMetric{{
			GaugeMetric: launchbackoff.OfferingsUnavailable,
			Value:       1,
			Labels: map[string]string{
				metrics.InstanceTypeLabel: key.InstanceType,
				metrics.CapacityTypeLabel: key.CapacityType,
				metrics.ZoneLabel:         key.Zone,
			},
		}}
	}
	for uid, burst := range c.launchBackoff.ConstrainedPools() {
		// The tracker budgets by UID because a NodePool recreated under the same name is a different
		// pool, but a UID is meaningless on a dashboard. A pool whose object is already gone is
		// skipped rather than labeled with its UID; it ages out of the tracker on its own.
		name, ok := nodePoolNames[uid]
		if !ok {
			continue
		}
		metricsMap[fmt.Sprintf("nodepool/%s", uid)] = []*metrics.StoreMetric{
			{
				GaugeMetric: launchbackoff.NodePoolsLaunchConstrained,
				Value:       1,
				Labels:      map[string]string{metrics.NodePoolLabel: name},
			},
			{
				GaugeMetric: launchbackoff.NodePoolsLaunchBurst,
				Value:       float64(burst),
				Labels:      map[string]string{metrics.NodePoolLabel: name},
			},
		}
	}
	c.metricStore.ReplaceAll(metricsMap)

	return reconciler.Result{RequeueAfter: pollInterval}, nil
}

func (c *Controller) nodePoolNames(ctx context.Context) (map[types.UID]string, error) {
	nodePoolList := &v1.NodePoolList{}
	if err := c.kubeClient.List(ctx, nodePoolList); err != nil {
		return nil, err
	}
	names := make(map[types.UID]string, len(nodePoolList.Items))
	for i := range nodePoolList.Items {
		names[nodePoolList.Items[i].UID] = nodePoolList.Items[i].Name
	}
	return names, nil
}

func (c *Controller) Register(_ context.Context, m manager.Manager) error {
	return controllerruntime.NewControllerManagedBy(m).
		Named(c.Name()).
		WatchesRawSource(singleton.Source()).
		Complete(singleton.AsReconciler(c))
}

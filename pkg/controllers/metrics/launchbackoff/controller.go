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
	"k8s.io/apimachinery/pkg/util/sets"
	controllerruntime "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/manager"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/metrics"
	"sigs.k8s.io/karpenter/pkg/operator/injection"
	"sigs.k8s.io/karpenter/pkg/state/launchbackoff"
)

// pollInterval bounds how stale a gauge can be. Backoff windows start at 30s, so a shorter
// interval would mostly resample unchanged state and a much longer one could miss a whole window.
const pollInterval = 5 * time.Second

// Controller publishes the launch backoff state gauges.
//
// A polling singleton rather than gauge writes at the tracker's mutation sites, because an
// offering's refill can become due without a corresponding event. Polling also lets the whole
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
	c.launchBackoff.Cleanup()
	boundReservations := c.launchBackoff.BoundReservations()
	if len(boundReservations) != 0 {
		nodeClaims := &v1.NodeClaimList{}
		if err := c.kubeClient.List(ctx, nodeClaims); err != nil {
			return reconciler.Result{}, fmt.Errorf("listing nodeclaims, %w", err)
		}
		liveNodeClaims := sets.New[types.UID]()
		for i := range nodeClaims.Items {
			liveNodeClaims.Insert(nodeClaims.Items[i].UID)
		}
		c.launchBackoff.ReleaseOrphanedBoundReservations(boundReservations, liveNodeClaims)
	}

	metricsMap := map[string][]*metrics.StoreMetric{}
	budgets := c.launchBackoff.Budgets()
	for key, budget := range budgets {
		labels := map[string]string{
			metrics.InstanceTypeLabel: key.InstanceType,
			metrics.CapacityTypeLabel: key.CapacityType,
			metrics.ZoneLabel:         key.Zone,
		}
		series := []*metrics.StoreMetric{{
			GaugeMetric: launchbackoff.OfferingsLaunchBudget,
			Value:       float64(budget.Burst),
			Labels:      labels,
		}}
		if budget.Unavailable {
			series = append(series, &metrics.StoreMetric{
				GaugeMetric: launchbackoff.OfferingsUnavailable,
				Value:       1,
				Labels:      labels,
			})
		}
		metricsMap[fmt.Sprintf("offering/%s/%s/%s", key.InstanceType, key.CapacityType, key.Zone)] = series
	}
	metricsMap["active-offerings"] = []*metrics.StoreMetric{{
		GaugeMetric: launchbackoff.ActiveOfferings,
		Value:       float64(len(budgets)),
		Labels:      map[string]string{},
	}}
	c.metricStore.ReplaceAll(metricsMap)

	return reconciler.Result{RequeueAfter: pollInterval}, nil
}

func (c *Controller) Register(_ context.Context, m manager.Manager) error {
	return controllerruntime.NewControllerManagedBy(m).
		Named(c.Name()).
		WatchesRawSource(singleton.Source()).
		Complete(singleton.AsReconciler(c))
}

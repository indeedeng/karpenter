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

package lifecycle

import (
	"context"
	"errors"
	"fmt"

	"github.com/awslabs/operatorpkg/object"
	"github.com/awslabs/operatorpkg/status"
	"github.com/patrickmn/go-cache"
	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/utils/clock"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/events"
	"sigs.k8s.io/karpenter/pkg/metrics"
	"sigs.k8s.io/karpenter/pkg/state/launchbackoff"
	nodeclaimutils "sigs.k8s.io/karpenter/pkg/utils/nodeclaim"
)

type Launch struct {
	kubeClient    client.Client
	cloudProvider cloudprovider.CloudProvider
	cache         *cache.Cache // exists due to eventual consistency on the cache
	recorder      events.Recorder
	clock         clock.Clock
	launchBackoff *launchbackoff.Tracker
}

func (l *Launch) Reconcile(ctx context.Context, nodeClaim *v1.NodeClaim) (reconcile.Result, error) {
	if cond := nodeClaim.StatusConditions().Get(v1.ConditionTypeLaunched); !cond.IsUnknown() {
		// Ensure that we always set the status condition to the latest generation
		nodeClaim.StatusConditions(status.WithClock(l.clock)).Set(*cond)
		if cond.IsTrue() {
			// Once the NodeClaim has successfully marked as launched, we no longer need to store it
			l.cache.Delete(string(nodeClaim.UID))
		}
		return reconcile.Result{}, nil
	}

	var err error
	var created *v1.NodeClaim

	// One of the following scenarios can happen with a NodeClaim that isn't marked as launched:
	//  1. It was already launched by the CloudProvider but the client-go cache wasn't updated quickly enough or
	//     patching failed on the status. In this case, we use the in-memory cached value for the created NodeClaim.
	//  2. It is a standard NodeClaim launch where we should call CloudProvider Create() and fill in details of the launched
	//     NodeClaim into the NodeClaim CR.
	if ret, ok := l.cache.Get(string(nodeClaim.UID)); ok {
		created = ret.(*v1.NodeClaim)
	} else {
		created, err = l.launchNodeClaim(ctx, nodeClaim)
	}
	// Either the Node launch failed or the Node was deleted due to InsufficientCapacity/NodeClassNotReady/NotFound
	if err != nil || created == nil {
		return reconcile.Result{}, err
	}
	l.cache.SetDefault(string(nodeClaim.UID), created)
	nodeClaim = PopulateNodeClaimDetails(nodeClaim, created)
	nodeClaim.StatusConditions(status.WithClock(l.clock)).SetTrue(v1.ConditionTypeLaunched)
	return reconcile.Result{}, nil
}

func (l *Launch) launchNodeClaim(ctx context.Context, nodeClaim *v1.NodeClaim) (*v1.NodeClaim, error) {
	created, err := l.cloudProvider.Create(ctx, nodeClaim)
	if err != nil {
		switch {
		case cloudprovider.IsInsufficientCapacityError(err):
			l.recorder.Publish(InsufficientCapacityErrorEvent(nodeClaim, err))
			log.FromContext(ctx).Error(err, "failed launching nodeclaim")
			// Recorded before the delete below, which returns early on error and would otherwise drop
			// the only evidence that this pool is short.
			l.observeLaunchFailure(ctx, nodeClaim, err)

			if err = l.kubeClient.Delete(ctx, nodeClaim); err != nil {
				return nil, client.IgnoreNotFound(err)
			}
			metrics.NodeClaimsDisruptedTotal.Inc(map[string]string{
				metrics.ReasonLabel:              metrics.InsufficientCapacityReason,
				metrics.NodePoolLabel:            nodeClaim.Labels[v1.NodePoolLabelKey],
				metrics.CapacityTypeLabel:        nodeClaim.Labels[v1.CapacityTypeLabelKey],
				metrics.ConsolidationPolicyLabel: "",
				metrics.TerminationModeLabel:     nodeclaimutils.DisruptionTerminationMode(nodeClaim),
			})
			return nil, nil
		case cloudprovider.IsNodeClassNotReadyError(err):
			log.FromContext(ctx).Error(err, "failed launching nodeclaim")
			if err = l.kubeClient.Delete(ctx, nodeClaim); err != nil {
				return nil, client.IgnoreNotFound(err)
			}
			metrics.NodeClaimsDisruptedTotal.Inc(map[string]string{
				metrics.ReasonLabel:              metrics.NodeClassNotReadyReason,
				metrics.NodePoolLabel:            nodeClaim.Labels[v1.NodePoolLabelKey],
				metrics.CapacityTypeLabel:        nodeClaim.Labels[v1.CapacityTypeLabelKey],
				metrics.ConsolidationPolicyLabel: "",
				metrics.TerminationModeLabel:     nodeclaimutils.DisruptionTerminationMode(nodeClaim),
			})
			return nil, nil
		default:
			var createError *cloudprovider.CreateError
			if errors.As(err, &createError) {
				nodeClaim.StatusConditions(status.WithClock(l.clock)).SetUnknownWithReason(v1.ConditionTypeLaunched, createError.ConditionReason, createError.ConditionMessage)
			} else {
				nodeClaim.StatusConditions(status.WithClock(l.clock)).SetUnknownWithReason(v1.ConditionTypeLaunched, "LaunchFailed", truncateMessage(err.Error()))
			}
			return nil, fmt.Errorf("launching nodeclaim, %w", err)
		}
	}

	l.observeLaunchSuccess(ctx, nodeClaim, created)

	log.FromContext(ctx).WithValues(
		"provider-id", created.Status.ProviderID,
		"instance-type", created.Labels[corev1.LabelInstanceTypeStable],
		"zone", created.Labels[corev1.LabelTopologyZone],
		"capacity-type", created.Labels[v1.CapacityTypeLabelKey],
		"allocatable", created.Status.Allocatable).Info("launched nodeclaim")
	return created, nil
}

// observeLaunchFailure feeds an insufficient capacity failure back to the launch backoff tracker.
//
// The keys the provider attributed back off exactly the pools that refused. The NodePool budget is
// armed regardless, so a provider that attributes nothing still gets a bound on how fast Karpenter
// retries — it just bounds the whole NodePool rather than the pools actually short.
func (l *Launch) observeLaunchFailure(ctx context.Context, nodeClaim *v1.NodeClaim, err error) {
	var ice *cloudprovider.InsufficientCapacityError
	var keys []cloudprovider.OfferingKey
	if errors.As(err, &ice) {
		keys = ice.Keys
	}
	for _, key := range keys {
		l.launchBackoff.Fail(ctx, key)
	}
	l.launchBackoff.FailPool(ctx, nodePoolUID(nodeClaim))

	// Counted whether or not the gate is on. An operator deciding whether to enable backoff needs to
	// see the failure rate it would be acting on first.
	if len(keys) == 0 {
		launchbackoff.OfferingsLaunchFailuresTotal.Inc(map[string]string{
			metrics.InstanceTypeLabel: "",
			metrics.CapacityTypeLabel: "",
			metrics.ZoneLabel:         "",
		})
		return
	}
	for _, key := range keys {
		launchbackoff.OfferingsLaunchFailuresTotal.Inc(map[string]string{
			metrics.InstanceTypeLabel: key.InstanceType,
			metrics.CapacityTypeLabel: key.CapacityType,
			metrics.ZoneLabel:         key.Zone,
		})
	}
}

// observeLaunchSuccess clears backoff for the pool that just produced an instance. The labels come
// from the created NodeClaim rather than the requested one, because the requested one carries the
// set of offerings the launch was allowed to draw from, not the one it landed in.
func (l *Launch) observeLaunchSuccess(ctx context.Context, nodeClaim, created *v1.NodeClaim) {
	l.launchBackoff.Succeed(ctx, cloudprovider.OfferingKey{
		InstanceType: created.Labels[corev1.LabelInstanceTypeStable],
		CapacityType: created.Labels[v1.CapacityTypeLabelKey],
		Zone:         created.Labels[corev1.LabelTopologyZone],
	})
	l.launchBackoff.SucceedPool(ctx, nodePoolUID(nodeClaim))
}

// nodePoolUID reads the owning NodePool's UID off the NodeClaim. Returns empty for a NodeClaim with
// no NodePool owner, which the tracker treats as nothing to record.
func nodePoolUID(nodeClaim *v1.NodeClaim) types.UID {
	nodePoolKind := object.GVK(&v1.NodePool{}).Kind
	for _, ref := range nodeClaim.OwnerReferences {
		if ref.Kind == nodePoolKind {
			return ref.UID
		}
	}
	return ""
}

func PopulateNodeClaimDetails(nodeClaim, retrieved *v1.NodeClaim) *v1.NodeClaim {
	// These are ordered in priority order so that user-defined nodeClaim labels and requirements trump retrieved labels
	// or the static nodeClaim labels
	nodeClaim.Labels = lo.Assign(
		retrieved.Labels, // CloudProvider-resolved labels
		nodeClaim.Labels, // User-defined labels
	)
	nodeClaim.Annotations = lo.Assign(nodeClaim.Annotations, retrieved.Annotations)
	nodeClaim.Status.ProviderID = retrieved.Status.ProviderID
	nodeClaim.Status.ImageID = retrieved.Status.ImageID
	nodeClaim.Status.Allocatable = retrieved.Status.Allocatable
	nodeClaim.Status.Capacity = retrieved.Status.Capacity
	return nodeClaim
}

func truncateMessage(msg string) string {
	if len(msg) < 300 {
		return msg
	}
	return msg[:300] + "..."
}

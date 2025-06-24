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
	"fmt"
	"strings"

	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/events"
)

func InsufficientCapacityErrorEvents(ctx context.Context, kubeClient client.Client, nodeClaim *v1.NodeClaim, err error) []events.Event {
	errMsg := truncateMessage(err.Error())
	evnts := []events.Event{{
		InvolvedObject: nodeClaim,
		Type:           corev1.EventTypeWarning,
		Reason:         events.InsufficientCapacityError,
		Message:        fmt.Sprintf("NodeClaim %s event: %s", nodeClaim.Name, errMsg),
		DedupeValues:   []string{string(nodeClaim.UID)},
	}}
	if nodeClaim.Annotations != nil && nodeClaim.Annotations["karpenter.indeed.com/nominated-pods"] != "" {
		evnts = append(evnts, nominatedPodEvents(ctx, kubeClient, nodeClaim, errMsg)...)
	}
	return evnts
}

func nominatedPodEvents(ctx context.Context, kubeClient client.Client, nodeClaim *v1.NodeClaim, errMsg string) []events.Event {
	awsZone := extractRequirementValue(nodeClaim, "topology.kubernetes.io/zone", "any az")
	instanceTypes := extractRequirementValue(nodeClaim, "node.kubernetes.io/instance-type", "")

	var evnts []events.Event
	pods := strings.Split(nodeClaim.Annotations["karpenter.indeed.com/nominated-pods"], ",")
	for _, podAndNS := range pods {
		split := strings.Split(podAndNS, "/")
		if len(split) != 2 {
			continue
		}
		pod := &corev1.Pod{}
		err := kubeClient.Get(ctx, client.ObjectKey{Namespace: strings.TrimSpace(split[0]), Name: strings.TrimSpace(split[1])}, pod)
		if err != nil {
			if !errors.IsNotFound(err) {
				log.FromContext(ctx).Error(err, "Failed to get pod")
			}
			continue
		}
		evnts = append(evnts, events.Event{
			InvolvedObject: pod,
			Type:           corev1.EventTypeWarning,
			Reason:         events.InsufficientCapacityError,
			Message:        fmt.Sprintf("Pod could not schedule %s in %s: %s", instanceTypes, awsZone, errMsg),
			DedupeValues:   []string{events.InsufficientCapacityError + string(pod.UID)},
		})
	}
	return evnts
}

func extractRequirementValue(nodeClaim *v1.NodeClaim, key, defaultValue string) string {
	for _, requirement := range nodeClaim.Spec.Requirements {
		if requirement.Key == key && len(requirement.Values) > 0 {
			return strings.Join(requirement.Values, ",")
		}
	}
	return defaultValue
}

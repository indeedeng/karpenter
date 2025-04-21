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
	"strings"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/controllers/provisioning/scheduling"
)

func annotateNodeClaimWithNominatedPods(
	n *scheduling.NodeClaim,
	nodeClaim *v1.NodeClaim,
) {
	var str strings.Builder
	for _, pod := range n.Pods {
		str.WriteString(pod.Namespace)
		str.WriteString("/")
		str.WriteString(pod.Name)
		str.WriteString(",")
	}
	nominatedPods := str.String()
	if len(nominatedPods) > 4096 {
		nominatedPods = nominatedPods[:4096]
	}
	// avoid datarace and make a copy
	annotations := make(map[string]string, len(nodeClaim.Annotations)+1)
	for k, v := range nodeClaim.Annotations {
		annotations[k] = v
	}
	annotations["karpenter.indeed.com/nominated-pods"] = nominatedPods
	nodeClaim.Annotations = annotations
}

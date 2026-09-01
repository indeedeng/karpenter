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
	"errors"

	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"

	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/cloudprovider/fake"
	"sigs.k8s.io/karpenter/pkg/scheduling"
)

// These exercise the classification in filterInstanceTypesByRequirements directly. The point of the
// type is that the provisioner can tell "wait for a backoff window" apart from "this pod needs a
// cluster change," so each spec pins down one side of that line.
//
// Availability is set on the offering rather than through the backoff tracker because the filter
// only ever sees the Available flag — clearing it is exactly what launchbackoff.FilterUnavailable
// does upstream, so this covers both causes.
var _ = Describe("OfferingsUnavailableError", func() {
	const zone = "test-zone-1"

	offering := func(available bool) cloudprovider.Offering {
		return cloudprovider.Offering{
			Requirements: scheduling.NewRequirements(
				scheduling.NewRequirement(v1.CapacityTypeLabelKey, corev1.NodeSelectorOpIn, v1.CapacityTypeOnDemand),
				scheduling.NewRequirement(corev1.LabelTopologyZone, corev1.NodeSelectorOpIn, zone),
			),
			Price:     1.0,
			Available: available,
		}
	}

	instanceType := func(available bool) *cloudprovider.InstanceType {
		// Always built from an available offering, then cleared, because fake.NewInstanceType
		// derives its zone and capacity-type requirements from Offerings.Available(). Constructing
		// it unavailable would produce a type that no longer advertises the zone at all, which is
		// not what the real path does: FilterUnavailable clears the flag and deliberately leaves
		// Requirements alone, so the pod still matches the type and only the offering is missing.
		it := fake.NewInstanceType("default-instance-type", fake.WithOfferings(offering(true)))
		if !available {
			it.Offerings[0].Available = false
		}
		return it
	}

	// requirementsFor mirrors what a pod with no placement preferences resolves to for this pool.
	requirementsFor := func(zoneName string) scheduling.Requirements {
		return scheduling.NewRequirements(
			scheduling.NewRequirement(v1.CapacityTypeLabelKey, corev1.NodeSelectorOpIn, v1.CapacityTypeOnDemand),
			scheduling.NewRequirement(corev1.LabelTopologyZone, corev1.NodeSelectorOpIn, zoneName),
		)
	}

	// filter runs the real filter over a single instance type with one daemon overhead group, the
	// shape buildDaemonOverheadGroups produces for a cluster with no DaemonSets.
	filter := func(it *cloudprovider.InstanceType, reqs scheduling.Requirements, requests corev1.ResourceList) error {
		its := []*cloudprovider.InstanceType{it}
		groups := []DaemonOverheadGroup{{InstanceTypes: its, HostPortUsage: scheduling.NewHostPortUsage()}}
		_, _, err := filterInstanceTypesByRequirements(its, reqs, &corev1.Pod{}, requests, groups, requests, false)
		return err
	}

	fits := corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1")}

	It("should classify a failure whose only cause is an unavailable offering", func() {
		err := filter(instanceType(false), requirementsFor(zone), fits)

		Expect(err).To(HaveOccurred())
		Expect(IsOfferingsUnavailableError(err)).To(BeTrue())
	})
	It("should not classify a failure on requirements", func() {
		// A zone this NodePool cannot launch in. Capacity is fine, and no backoff window elapsing
		// will make this pod schedulable.
		err := filter(instanceType(true), requirementsFor("test-zone-99"), fits)

		Expect(err).To(HaveOccurred())
		Expect(IsOfferingsUnavailableError(err)).To(BeFalse())
	})
	It("should not classify a failure on resources", func() {
		tooBig := corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("1000000")}

		err := filter(instanceType(true), requirementsFor(zone), tooBig)

		Expect(err).To(HaveOccurred())
		Expect(IsOfferingsUnavailableError(err)).To(BeFalse())
	})
	It("should not classify a filter that succeeded", func() {
		Expect(filter(instanceType(true), requirementsFor(zone), fits)).To(Succeed())
		Expect(IsOfferingsUnavailableError(nil)).To(BeFalse())
	})
	It("should keep the underlying error reachable and its message intact", func() {
		err := filter(instanceType(false), requirementsFor(zone), fits)

		// The wrapper exists for classification only. Operators still need the diagnostic text,
		// and anything already matching on InstanceTypeFilterError has to keep working.
		Expect(err.Error()).To(ContainSubstring("offering"))
		var filterErr InstanceTypeFilterError
		Expect(errors.As(err, &filterErr)).To(BeTrue())
	})
})

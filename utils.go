/*
 * Copyright (c) 2019, NVIDIA CORPORATION.  All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package main

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/NVIDIA/gpu-monitoring-tools/bindings/go/nvml"
	"github.com/prometheus/common/log"
	v1 "k8s.io/api/core/v1"
	"k8s.io/klog"
	pluginapi "k8s.io/kubelet/pkg/apis/deviceplugin/v1beta1"
)

//Container specific operations

func IsGPURequiredContainer(c *v1.Container) bool {

	vmemory := GetGPUResourceOfContainer(c, VMemoryAnnotationConst)

	if vmemory <= 0 {
		klog.V(4).Infof("container don't need gpu resource")
		return false
	}

	return true
}

func GetGPUResourceOfContainer(container *v1.Container, resourceName v1.ResourceName) uint {
	var count uint
	if val, ok := container.Resources.Limits[resourceName]; ok {
		count = uint(val.Value())
	}
	return count
}

func GetContainerIndexByName(pod *v1.Pod, containerName string) (int, error) {
	containerIndex := -1
	for i, c := range pod.Spec.Containers {
		if c.Name == containerName {
			containerIndex = i
			break
		}
	}

	if containerIndex == -1 {
		return containerIndex, fmt.Errorf("failed to get index of container %s in pod %s", containerName, pod.UID)
	}
	return containerIndex, nil
}

////Device specific operations

var (
	gpuMemory uint
)

func GenerateFakeDeviceID(realID string, fakeCounter uint) string {
	return fmt.Sprintf("%s-_-%d", realID, fakeCounter)
}

func ExtractRealDeviceID(fakeDeviceID string) string {
	return strings.Split(fakeDeviceID, "-_-")[0]
}

func SetGPUMemory(raw uint) {
	v := raw
	gpuMemory = v
	log.Infof("set gpu memory: %d", gpuMemory)
}

func GetGPUMemory() uint {
	return gpuMemory
}

func GetDevices() ([]*pluginapi.Device, map[uint]string) {
	n, err := nvml.GetDeviceCount()
	check(err)

	var devs []*pluginapi.Device
	deviceByIndex := map[uint]string{}
	for i := uint(0); i < n; i++ {
		d, err := nvml.NewDevice(i)
		check(err)
		var id uint
		_, err = fmt.Sscanf(d.Path, "/dev/nvidia%d", &id)
		check(err)
		deviceByIndex[id] = d.UUID
		// TODO: Do we assume all cards are of same capacity
		if GetGPUMemory() == uint(0) {
			SetGPUMemory(uint(*d.Memory))
		}
		for j := uint(0); j < GetGPUMemory(); j++ {
			fakeID := GenerateFakeDeviceID(d.UUID, j)
			devs = append(devs, &pluginapi.Device{
				ID:     fakeID,
				Health: pluginapi.Healthy,
			})
		}
	}

	return devs, deviceByIndex
}

//Pod specific operations

type podSlice []*v1.Pod

func (s podSlice) Len() int {
	return len(s)
}

func (s podSlice) Less(i, j int) bool {
	return GetPredicateTimeFromPodAnnotation(s[i]) <= GetPredicateTimeFromPodAnnotation(s[j])
}

func (s podSlice) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func IsGPURequiredPod(pod *v1.Pod) bool {

	vmemory := GetGPUResourceOfPod(pod, VMemoryAnnotationConst)

	if vmemory <= 0 {
		return false
	}

	return true
}

func IsGPUAssignedPod(pod *v1.Pod) bool {

	if assigned, ok := pod.ObjectMeta.Annotations[GPUAssignedConst]; !ok {
		klog.V(4).Infof("no assigned flag",
			pod.Name,
			pod.Namespace)
		return false
	} else if assigned == "false" {
		klog.V(4).Infof("pod has not been assigned",
			pod.Name,
			pod.Namespace)
		return false
	}

	return true
}

func IsShouldDeletePod(pod *v1.Pod) bool {
	for _, status := range pod.Status.ContainerStatuses {
		if status.State.Waiting != nil &&
			strings.Contains(status.State.Waiting.Message, "PreStartContainer check failed") {
			return true
		}
	}
	if pod.Status.Reason == "UnexpectedAdmissionError" {
		return true
	}
	return false
}

func GetGPUResourceOfPod(pod *v1.Pod, resourceName v1.ResourceName) uint {
	var total uint
	containers := pod.Spec.Containers
	for _, container := range containers {
		if val, ok := container.Resources.Limits[resourceName]; ok {
			total += uint(val.Value())
		}
	}
	return total
}

func GetPredicateTimeFromPodAnnotation(pod *v1.Pod) (assumeTime uint64) {
	if assumeTimeStr, ok := pod.ObjectMeta.Annotations[AssumedTimeEnv]; ok {
		u64, err := strconv.ParseUint(assumeTimeStr, 10, 64)
		if err == nil {
			assumeTime = u64
		}
	}

	return assumeTime
}

// GetGPUIDFromPodAnnotation returns the ID of the GPU if allocated
func GetGPUIDFromPodAnnotation(pod *v1.Pod) (int, bool) {
	if len(pod.ObjectMeta.Annotations) > 0 {
		value, found := pod.ObjectMeta.Annotations[ResourceIndexEnv]
		if found {
			id, err := strconv.Atoi(value)
			if err != nil {
				klog.Error("invalid ResourceIndexEnv=%s", value)
				return 0, false
			}
			return id, true
		}
	}

	return 0, false
}

func UpdatePodAnnotations(pod *v1.Pod) {
	if len(pod.ObjectMeta.Annotations) == 0 {
		pod.ObjectMeta.Annotations = map[string]string{}
	}

	now := time.Now()
	pod.ObjectMeta.Annotations[GPUAssignedConst] = "true"
	pod.ObjectMeta.Annotations[AssumedTimeEnv] = fmt.Sprintf("%d", now.UnixNano())
}

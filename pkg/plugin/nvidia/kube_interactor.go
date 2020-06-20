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

package nvidia

import (
	"context"
	"fmt"
	"os"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/tools/clientcmd"
	"k8s.io/klog"
	nodeutil "k8s.io/kubernetes/pkg/util/node"
)

type KubeInteractor struct {
	clientset *kubernetes.Clientset
	nodeName  string
}

func NewKubeClient() (*kubernetes.Clientset, error) {
	clientCfg, err := clientcmd.BuildConfigFromFlags("", os.Getenv("KUBECONFIG"))
	if err != nil {
		return nil, err
	}

	clientset, err := kubernetes.NewForConfig(clientCfg)
	if err != nil {
		return nil, err
	}

	return clientset, nil
}

func NewKubeInteractor() (*KubeInteractor, error) {
	client, err := NewKubeClient()
	if err != nil {
		return nil, fmt.Errorf("cannot create kube client: %v", err)
	}

	return &KubeInteractor{
		clientset: client,
		nodeName:  os.Getenv("NODE_NAME"),
	}, nil
}

func (ki *KubeInteractor) GetPendingPodsOnNode() ([]v1.Pod, error) {
	var (
		res []v1.Pod
		pl  *v1.PodList
		err error
	)
	selector := fields.SelectorFromSet(fields.Set{"spec.nodeName": ki.nodeName, "status.phase": string(v1.PodPending)})
	err = wait.PollImmediate(1*time.Second, 10*time.Second, func() (bool, error) {
		pl, err = ki.clientset.CoreV1().Pods(v1.NamespaceAll).List(context.TODO(), metav1.ListOptions{
			FieldSelector: selector.String(),
		})
		if err != nil {
			return false, nil
		}
		return true, nil
	})
	if err != nil {
		return nil, fmt.Errorf("kube interactor timedout: %v", err)
	}

	for _, pod := range pl.Items {
		res = append(res, pod)
	}

	return res, nil
}

func (ki *KubeInteractor) PatchGPUCountOnNode(gpuCount int) error {

	var (
		node *v1.Node
		err  error
	)

	err = wait.PollImmediate(1*time.Second, 10*time.Second, func() (bool, error) {
		node, err = ki.clientset.CoreV1().Nodes().Get(context.TODO(), ki.nodeName, metav1.GetOptions{})

		if err != nil {
			return false, err
		}
		return true, nil
	})

	if err != nil {
		return fmt.Errorf("kube interactor timedout: %v", err)
	}

	newNode := node.DeepCopy()
	newNode.Status.Capacity[GpuCoreConst] = *resource.NewQuantity(int64(gpuCount), resource.DecimalSI)
	newNode.Status.Allocatable[GpuCoreConst] = *resource.NewQuantity(int64(gpuCount), resource.DecimalSI)
	_, _, err = nodeutil.PatchNodeStatus(ki.clientset.CoreV1(), types.NodeName(ki.nodeName), node, newNode)

	if err != nil {
		klog.Infof("failed to update %s.", GpuCoreConst)
		return err
	}

	return nil

}

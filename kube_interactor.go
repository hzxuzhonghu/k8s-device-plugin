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
	"context"
	"fmt"
	"os"
	"time"

	glog "github.com/golang/glog"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/fields"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	nodeutil "k8s.io/kubernetes/pkg/util/node"
)

type KubeInteractor struct {
	clientset *kubernetes.Clientset
	hostname  string
}

func NewKubeClient() (*kubernetes.Clientset, error) {
	var (
		err       error
		clientCfg *rest.Config
		clientset *kubernetes.Clientset
	)

	if _, err = os.Stat(os.Getenv("KUBECONFIG")); err != nil {
		clientCfg, err = rest.InClusterConfig()
		if err != nil {
			glog.Fatalf("Failed to get config from default. %v", err)
		}
	} else {
		clientCfg, err = clientcmd.BuildConfigFromFlags("", os.Getenv("KUBECONFIG"))
		if err != nil {
			glog.Fatalf("Failed to get config from env. %v", err)
		}
	}

	clientset, err = kubernetes.NewForConfig(clientCfg)
	if err != nil {
		glog.Fatalf("Failed to create clientset. %v", err)
	}

	return clientset, nil

}

func NewKubeInteractor() (*KubeInteractor, error) {
	var ki *KubeInteractor

	if kc, err := NewKubeClient(); err != nil {
		glog.Fatalf("cannot create kube client. %v", err)
	} else {
		ki.clientset = kc
	}

	if hn, err := os.Hostname(); err != nil {
		glog.Fatalf("cannot get hostname from os. %v", err)
	} else {
		ki.hostname = hn
	}

	return ki, nil
}

func (ki *KubeInteractor) GetSpecificStatusPodsOnNode(status string) ([]v1.Pod, error) {

	selector := fields.SelectorFromSet(fields.Set{"spec.nodeName": ki.hostname, "status.phase": status})

	var (
		res []v1.Pod
		pl  *v1.PodList
		err error
	)

	err = wait.PollImmediate(1*time.Second, 10*time.Second, func() (bool, error) {
		pl, err = ki.clientset.CoreV1().Pods(v1.NamespaceAll).List(context.TODO(), metav1.ListOptions{
			FieldSelector: selector.String(),
			LabelSelector: labels.Everything().String(),
		})
		if err != nil {
			return false, err
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
		node, err = ki.clientset.CoreV1().Nodes().Get(context.TODO(), ki.hostname, metav1.GetOptions{})

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
	_, _, err = nodeutil.PatchNodeStatus(ki.clientset.CoreV1(), types.NodeName(ki.hostname), node, newNode)

	if err != nil {
		glog.Infof("failed to update %s.", GpuCoreConst)
		return err
	}

	return nil

}

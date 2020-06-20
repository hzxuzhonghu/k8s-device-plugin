/*
Copyright 2020 The Volcano Authors.

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

package nvidia

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"path"
	"sort"
	"time"

	"github.com/golang/glog"
	v1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/klog"

	"golang.org/x/net/context"
	"google.golang.org/grpc"
	pluginapi "k8s.io/kubelet/pkg/apis/deviceplugin/v1beta1"
)

var passDeviceSpecs = flag.Bool("pass-device-specs", false, "pass the list of DeviceSpecs to the kubelet on Allocate()")

// NvidiaDevicePlugin implements the Kubernetes device plugin API
type NvidiaDevicePlugin struct {
	ResourceManager
	resourceName   string
	allocateEnvvar string
	socket         string

	server        *grpc.Server
	cachedDevices []*Device
	health        chan *Device
	stop          chan struct{}

	devMap         map[uint]string
	kubeInteractor *KubeInteractor
}

// NewNvidiaDevicePlugin returns an initialized NvidiaDevicePlugin
func NewNvidiaDevicePlugin(resourceName string, allocateEnvvar string, socket string) *NvidiaDevicePlugin {

	ki, err := NewKubeInteractor()
	if err != nil {
		klog.Fatalf("cannot create kube interactor. %v", err)
	}

	return &NvidiaDevicePlugin{
		ResourceManager: NewGpuDeviceManager(),
		resourceName:    resourceName,
		allocateEnvvar:  allocateEnvvar,
		socket:          socket,
		kubeInteractor:  ki,

		// These will be reinitialized every
		// time the plugin server is restarted.
		cachedDevices: nil,
		server:        nil,
		health:        nil,
		stop:          nil,
		devMap:        nil,
	}
}

func (m *NvidiaDevicePlugin) initialize() {
	m.cachedDevices = m.Devices()
	m.server = grpc.NewServer([]grpc.ServerOption{}...)
	m.health = make(chan *Device)
	m.stop = make(chan struct{})

	_, devMap := GetDevices()
	m.devMap = devMap

}

func (m *NvidiaDevicePlugin) cleanup() {
	close(m.stop)
	m.cachedDevices = nil
	m.server = nil
	m.health = nil
	m.stop = nil

	m.devMap = nil
}

func (m *NvidiaDevicePlugin) GetDeviceNameByIndex(index uint) (name string, found bool) {
	if m.devMap != nil {
		name, ok := m.devMap[index]
		return name, ok
	}
	return "", false
}

// Start starts the gRPC server, registers the device plugin with the Kubelet,
// and starts the device healthchecks.
func (m *NvidiaDevicePlugin) Start() error {
	m.initialize()

	err := m.Serve()
	if err != nil {
		log.Printf("Could not start device plugin for '%s': %s", m.resourceName, err)
		m.cleanup()
		return err
	}
	log.Printf("Starting to serve '%s' on %s", m.resourceName, m.socket)

	err = m.Register()
	if err != nil {
		log.Printf("Could not register device plugin: %s", err)
		m.Stop()
		return err
	}
	log.Printf("Registered device plugin for '%s' with Kubelet", m.resourceName)

	go m.CheckHealth(m.stop, m.cachedDevices, m.health)

	return nil
}

// Stop stops the gRPC server.
func (m *NvidiaDevicePlugin) Stop() error {
	if m == nil || m.server == nil {
		return nil
	}
	log.Printf("Stopping to serve '%s' on %s", m.resourceName, m.socket)
	m.server.GracefulStop()
	if err := os.Remove(m.socket); err != nil && !os.IsNotExist(err) {
		return err
	}
	m.cleanup()
	return nil
}

func (m *NvidiaDevicePlugin) DevicesNum() int {
	return len(m.Devices())
}

// Serve starts the gRPC server of the device plugin.
func (m *NvidiaDevicePlugin) Serve() error {
	sock, err := net.Listen("unix", m.socket)
	if err != nil {
		return err
	}

	pluginapi.RegisterDevicePluginServer(m.server, m)

	go func() {
		lastCrashTime := time.Now()
		restartCount := 0
		for {
			log.Printf("Starting GRPC server for '%s'", m.resourceName)
			err := m.server.Serve(sock)
			if err == nil {
				break
			}

			log.Printf("GRPC server for '%s' crashed with error: %v", m.resourceName, err)

			// restart if it has not been too often
			// i.e. if server has crashed more than 5 times and it didn't last more than one hour each time
			if restartCount > 5 {
				// quit
				log.Fatal("GRPC server for '%s' has repeatedly crashed recently. Quitting", m.resourceName)
			}
			timeSinceLastCrash := time.Since(lastCrashTime).Seconds()
			lastCrashTime = time.Now()
			if timeSinceLastCrash > 3600 {
				// it has been one hour since the last crash.. reset the count
				// to reflect on the frequency
				restartCount = 1
			} else {
				restartCount += 1
			}
		}
	}()

	// Wait for server to start by launching a blocking connection
	conn, err := m.dial(m.socket, 5*time.Second)
	if err != nil {
		return err
	}
	conn.Close()

	return nil
}

// Register registers the device plugin for the given resourceName with Kubelet.
func (m *NvidiaDevicePlugin) Register() error {
	conn, err := m.dial(pluginapi.KubeletSocket, 5*time.Second)
	if err != nil {
		return err
	}
	defer conn.Close()

	client := pluginapi.NewRegistrationClient(conn)
	reqt := &pluginapi.RegisterRequest{
		Version:      pluginapi.Version,
		Endpoint:     path.Base(m.socket),
		ResourceName: m.resourceName,
	}

	_, err = client.Register(context.Background(), reqt)
	if err != nil {
		return err
	}
	return nil
}

func (m *NvidiaDevicePlugin) GetDevicePluginOptions(context.Context, *pluginapi.Empty) (*pluginapi.DevicePluginOptions, error) {
	return &pluginapi.DevicePluginOptions{}, nil
}

// ListAndWatch lists devices and update that list according to the health status
func (m *NvidiaDevicePlugin) ListAndWatch(e *pluginapi.Empty, s pluginapi.DevicePlugin_ListAndWatchServer) error {
	s.Send(&pluginapi.ListAndWatchResponse{Devices: m.apiDevices()})

	for {
		select {
		case <-m.stop:
			return nil
		case d := <-m.health:
			// FIXME: there is no way to recover from the Unhealthy state.
			d.Health = pluginapi.Unhealthy
			log.Printf("'%s' device marked unhealthy: %s", m.resourceName, d.ID)
			s.Send(&pluginapi.ListAndWatchResponse{Devices: m.apiDevices()})
		}
	}
}

// Allocate which return list of devices.
func (m *NvidiaDevicePlugin) Allocate(ctx context.Context, reqs *pluginapi.AllocateRequest) (*pluginapi.AllocateResponse, error) {

	var reqCount uint

	for _, req := range reqs.ContainerRequests {
		reqCount += uint(len(req.DevicesIDs))
	}

	if len(reqs.ContainerRequests) == 0 {
		return nil, fmt.Errorf("No container request")
	}

	responses := pluginapi.AllocateResponse{}

	firstContainerReq := reqs.ContainerRequests[0]
	firstContainerReqDeviceCount := uint(len(firstContainerReq.DevicesIDs))

	availablePods := podSlice{}
	pendingPods, err := m.kubeInteractor.GetPendingPodsOnNode()
	if err != nil {
		return nil, err
	}
	for _, pod := range pendingPods {
		current := pod
		if IsGPURequiredPod(&current) && !IsGPUAssignedPod(&current) && !IsShouldDeletePod(&current) {
			availablePods = append(availablePods, &current)
		}
	}

	sort.Sort(availablePods)

	var candidatePod *v1.Pod
	for _, pod := range availablePods {
		for i, c := range pod.Spec.Containers {
			if !IsGPURequiredContainer(&c) {
				continue
			}

			// TODO: is this right?
			if GetGPUResourceOfContainer(&pod.Spec.Containers[i], VCoreAnnotationConst) == firstContainerReqDeviceCount {
				glog.Infof("Got candidate Pod %s(%s), the device count is: %d", pod.UID, c.Name, firstContainerReqDeviceCount)
				candidatePod = pod
				break
			}
		}
	}

	if candidatePod != nil {
		id, exist := GetGPUIDFromPodAnnotation(candidatePod)
		if !exist {
			klog.Warningf("Failed to get the gpu id for pod %s/%s", candidatePod.Namespace, candidatePod.Name)
			return genResponse(reqs, reqCount), nil
		}
		_, exist = m.GetDeviceNameByIndex(uint(id))
		if !exist {
			klog.Warningf("Failed to find the dev for pod %s/%s because it's not able to find dev with index %d",
				candidatePod.Namespace, candidatePod.Name, id)
			return genResponse(reqs, reqCount), nil
		}

		for _, req := range reqs.ContainerRequests {
			reqGPU := len(req.DevicesIDs)
			response := pluginapi.ContainerAllocateResponse{
				//TODO: (tizhou86) Add envs into response
				Envs: map[string]string{
					ContainerResourceEnv: fmt.Sprintf("%d", reqGPU),
				},
			}
			responses.ContainerResponses = append(responses.ContainerResponses, &response)
		}

		pod, err := m.kubeInteractor.clientset.CoreV1().Pods(candidatePod.Namespace).Get(context.TODO(), candidatePod.Name, metav1.GetOptions{})
		if err != nil {
			return genResponse(reqs, reqCount), nil
		}
		UpdatePodAnnotations(pod)
		_, err = m.kubeInteractor.clientset.CoreV1().Pods(pod.Namespace).Update(context.TODO(), pod, metav1.UpdateOptions{})
		if err != nil {
			return genResponse(reqs, reqCount), nil
		}

	} else {
		return genResponse(reqs, reqCount), nil
	}

	return &responses, nil

}

func (m *NvidiaDevicePlugin) PreStartContainer(context.Context, *pluginapi.PreStartContainerRequest) (*pluginapi.PreStartContainerResponse, error) {
	return &pluginapi.PreStartContainerResponse{}, nil
}

// dial establishes the gRPC communication with the registered device plugin.
func (m *NvidiaDevicePlugin) dial(unixSocketPath string, timeout time.Duration) (*grpc.ClientConn, error) {
	c, err := grpc.Dial(unixSocketPath, grpc.WithInsecure(), grpc.WithBlock(),
		grpc.WithTimeout(timeout),
		grpc.WithDialer(func(addr string, timeout time.Duration) (net.Conn, error) {
			return net.DialTimeout("unix", addr, timeout)
		}),
	)

	if err != nil {
		return nil, err
	}

	return c, nil
}

func (m *NvidiaDevicePlugin) deviceExists(id string) bool {
	for _, d := range m.cachedDevices {
		if d.ID == id {
			return true
		}
	}
	return false
}

func (m *NvidiaDevicePlugin) apiDevices() []*pluginapi.Device {
	var pdevs []*pluginapi.Device
	for _, d := range m.cachedDevices {
		pdevs = append(pdevs, &d.Device)
	}
	return pdevs
}

func (m *NvidiaDevicePlugin) apiDeviceSpecs(filter []string) []*pluginapi.DeviceSpec {
	var specs []*pluginapi.DeviceSpec

	paths := []string{
		"/dev/nvidiactl",
		"/dev/nvidia-uvm",
		"/dev/nvidia-uvm-tools",
		"/dev/nvidia-modeset",
	}

	for _, p := range paths {
		if _, err := os.Stat(p); err == nil {
			spec := &pluginapi.DeviceSpec{
				ContainerPath: p,
				HostPath:      p,
				Permissions:   "rw",
			}
			specs = append(specs, spec)
		}
	}

	for _, d := range m.cachedDevices {
		for _, id := range filter {
			if d.ID == id {
				spec := &pluginapi.DeviceSpec{
					ContainerPath: d.Path,
					HostPath:      d.Path,
					Permissions:   "rw",
				}
				specs = append(specs, spec)
			}
		}
	}

	return specs
}

func genResponse(reqs *pluginapi.AllocateRequest, podReqGPU uint) *pluginapi.AllocateResponse {
	responses := pluginapi.AllocateResponse{}
	for _, req := range reqs.ContainerRequests {
		response := pluginapi.ContainerAllocateResponse{
			//TODO: (tizhou86) Add envs into response
			Envs: map[string]string{
				ContainerResourceEnv: fmt.Sprintf("%d", uint(len(req.DevicesIDs))),
			},
		}
		responses.ContainerResponses = append(responses.ContainerResponses, &response)
	}
	return &responses
}

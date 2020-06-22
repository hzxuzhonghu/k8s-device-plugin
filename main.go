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
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/NVIDIA/gpu-monitoring-tools/bindings/go/nvml"
	"github.com/fsnotify/fsnotify"
	pluginapi "k8s.io/kubelet/pkg/apis/deviceplugin/v1beta1"

	"volcano.sh/k8s-device-plugin/pkg/filewatcher"
	"volcano.sh/k8s-device-plugin/pkg/plugin"
	"volcano.sh/k8s-device-plugin/pkg/plugin/nvidia"
)

func getAllPlugins() []plugin.DevicePlugin {
	return []plugin.DevicePlugin{
		nvidia.NewNvidiaDevicePlugin(
			nvidia.VolcanoGPUResource,
			"NVIDIA_VISIBLE_DEVICES",
			pluginapi.DevicePluginPath+"nvidia.sock"),
	}
}

func main() {
	flag.Parse()

	// TODO: move this down into nvidia device plugin
	log.Println("Loading NVML")
	if err := nvml.Init(); err != nil {
		log.Printf("Failed to initialize NVML: %s.", err)
		log.Printf("If this is a GPU node, did you set the docker default runtime to `nvidia`?")
		log.Printf("You can check the prerequisites at: https://github.com/NVIDIA/k8s-device-plugin#prerequisites")
		log.Printf("You can learn how to set the runtime at: https://github.com/NVIDIA/k8s-device-plugin#quick-start")

		select {}
	}
	defer func() { log.Println("Shutdown of NVML returned:", nvml.Shutdown()) }()

	log.Println("Starting FS watcher.")
	watcher, err := filewatcher.NewFSWatcher(pluginapi.DevicePluginPath)
	if err != nil {
		log.Println("Failed to created FS watcher.")
		os.Exit(1)
	}
	defer watcher.Close()

	log.Println("Starting OS watcher.")
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGHUP, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)

	log.Println("Retrieving plugins.")
	plugins := getAllPlugins()

restart:
	// Loop through all plugins, idempotently stopping them, and then starting
	// them if they have any devices to serve. If even one plugin fails to
	// start properly, try starting them all again.
	started := 0
	for _, p := range plugins {
		p.Stop()

		// Just continue if there are no devices to serve for plugin p.
		if p.DevicesNum() == 0 {
			continue
		}

		// Start the gRPC server for plugin p and connect it with the kubelet.
		if err := p.Start(); err != nil {
			log.Println("Could not contact Kubelet, retrying. Did you enable the device plugin feature gate?")
			log.Printf("You can check the prerequisites at: https://github.com/NVIDIA/k8s-device-plugin#prerequisites")
			log.Printf("You can learn how to set the runtime at: https://github.com/NVIDIA/k8s-device-plugin#quick-start")
			// If there was an error starting any plugins, restart them all.
			goto restart
		}
		started++
	}

	if started == 0 {
		log.Println("No devices found. Waiting indefinitely.")
	}

	// Start an infinite loop, waiting for several indicators to either log
	// some messages, trigger a restart of the plugins, or exit the program.
	for {
		select {
		// Detect a kubelet restart by watching for a newly created
		// 'pluginapi.KubeletSocket' file. When this occurs, restart this loop,
		// restarting all of the plugins in the process.
		case event := <-watcher.Events:
			if event.Name == pluginapi.KubeletSocket && event.Op&fsnotify.Create == fsnotify.Create {
				log.Printf("inotify: %s created, restarting.", pluginapi.KubeletSocket)
				goto restart
			}

		// Watch for any other fs errors and log them.
		case err := <-watcher.Errors:
			log.Printf("inotify: %s", err)

		// Watch for any signals from the OS. On SIGHUP, restart this loop,
		// restarting all of the plugins in the process. On all other
		// signals, exit the loop and exit the program.
		case s := <-sigCh:
			switch s {
			case syscall.SIGHUP:
				log.Println("Received SIGHUP, restarting.")
				goto restart
			default:
				log.Printf("Received signal \"%v\", shutting down.", s)
				for _, p := range plugins {
					p.Stop()
				}
				return
			}
		}
	}
}

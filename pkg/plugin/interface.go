package plugin

import pluginapi "k8s.io/kubelet/pkg/apis/deviceplugin/v1beta1"

// DevicePlugin interface
type DevicePlugin interface {
	// Start the plugin
	Start() error
	// Get all the devices number which reside within the node
	DevicesNum() int
	// Stop the plugin
	Stop() error
}

// Device indicates the detailed device info
type Device struct {
	// Device ID health status
	pluginapi.Device
	// Device path
	Path string
}

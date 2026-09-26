package resource

import (
	"fmt"
	"time"

	"github.com/NVIDIA/go-nvml/pkg/nvml"
)

// NVML reads one GPU by UUID. Process attribution is not applied; callers use zero
// when host processes cannot be tied to a runtime.
type NVML struct {
	Device string
}

func (n NVML) Observe() (Snapshot, error) {
	if n.Device == "" {
		return Snapshot{}, fmt.Errorf("nvml: device is empty")
	}
	if ret := nvml.Init(); ret != nvml.SUCCESS {
		return Snapshot{}, fmt.Errorf("nvml init: %s", nvml.ErrorString(ret))
	}
	dev, ret := nvml.DeviceGetHandleByUUID(n.Device)
	if ret != nvml.SUCCESS {
		return Snapshot{}, fmt.Errorf("nvml device %s: %s", n.Device, nvml.ErrorString(ret))
	}
	mem, ret := dev.GetMemoryInfo()
	if ret != nvml.SUCCESS {
		return Snapshot{}, fmt.Errorf("nvml memory: %s", nvml.ErrorString(ret))
	}
	uuid, ret := dev.GetUUID()
	if ret != nvml.SUCCESS {
		return Snapshot{}, fmt.Errorf("nvml uuid: %s", nvml.ErrorString(ret))
	}
	if uuid != n.Device {
		return Snapshot{}, fmt.Errorf("nvml uuid %s does not match %s", uuid, n.Device)
	}
	return Snapshot{
		DeviceID:   uuid,
		Total:      mem.Total,
		Free:       mem.Free,
		Used:       mem.Used,
		ObservedAt: time.Now(),
		OK:         true,
	}, nil
}

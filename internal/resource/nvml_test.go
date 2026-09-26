package resource

import (
	"os/exec"
	"strings"
	"testing"
	"time"
)

func TestNVMLObserve(t *testing.T) {
	out, err := exec.Command("nvidia-smi", "-L").Output()
	if err != nil {
		t.Skip("nvidia-smi unavailable")
	}
	line := string(out)
	i := strings.Index(line, "GPU-")
	if i < 0 {
		t.Skip("no GPU uuid")
	}
	uuid := line[i:]
	if nl := strings.IndexAny(uuid, " )\n"); nl > 0 {
		uuid = uuid[:nl]
	}
	snap, err := (NVML{Device: uuid}).Observe()
	if err != nil {
		t.Fatal(err)
	}
	if snap.DeviceID != uuid || snap.Total == 0 || !snap.OK {
		t.Fatalf("snap=%+v", snap)
	}
	if time.Since(snap.ObservedAt) > 5*time.Second {
		t.Fatalf("observedAt=%s", snap.ObservedAt)
	}
	if _, err := (NVML{Device: "GPU-does-not-exist"}).Observe(); err == nil {
		t.Fatal("mismatched uuid was accepted")
	}
}

package usage

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func write(t *testing.T, root, name, text string) {
	t.Helper()
	path := filepath.Join(root, name)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(text), 0o644); err != nil {
		t.Fatal(err)
	}
}

func v2(t *testing.T) string {
	root := t.TempDir()
	write(t, root, "cgroup.controllers", "cpu memory")
	write(t, root, "cpu.stat", "usage_usec 0\n")
	write(t, root, "cpu.max", "max 100000")
	write(t, root, "memory.current", "0")
	write(t, root, "memory.max", "max")
	write(t, root, "memory.stat", "anon 1\ninactive_file 0\n")
	return root
}

func noTools(context.Context, string, ...string) ([]byte, error) { return nil, errors.New("absent") }

func TestNamespacedContainerReadsItsOwnMemoryWithoutPageCache(t *testing.T) {
	root := v2(t)
	write(t, root, "memory.current", "3221225472")
	write(t, root, "memory.max", "4294967296")
	write(t, root, "memory.stat", "anon 1\ninactive_file 1073741824\n")
	m := &Meter{Root: root, Run: noTools}
	used, total := m.ram()
	if *used != 2<<30 || *total != 4<<30 {
		t.Fatalf("got %d of %d", *used, *total)
	}
}

func TestCPUIsMeasuredAgainstTheQuota(t *testing.T) {
	root := v2(t)
	write(t, root, "cpu.max", "200000 100000")
	clock := time.Unix(100, 0)
	m := &Meter{Root: root, Run: noTools, Cores: func() float64 { return 8 }, Now: func() time.Time { return clock }}
	if m.cpu() != nil {
		t.Fatal("first call has no delta")
	}
	write(t, root, "cpu.stat", "usage_usec 10000000\n")
	clock = clock.Add(10 * time.Second)
	if got := m.cpu(); got == nil || *got != 50 {
		t.Fatalf("got %v", got)
	}
}

func TestCoresFallBackToTheAllowedSet(t *testing.T) {
	root := v2(t)
	m := &Meter{Root: root, Cores: func() float64 { return 4 }}
	if m.cores() != 4 {
		t.Fatal(m.cores())
	}
	write(t, root, "cpu.max", "100000 100000")
	if m.cores() != 1 {
		t.Fatal(m.cores())
	}
}

func TestCgroupV1(t *testing.T) {
	root := t.TempDir()
	write(t, root, "memory/memory.limit_in_bytes", "2147483648")
	write(t, root, "memory/memory.usage_in_bytes", "1073741824")
	write(t, root, "memory/memory.stat", "total_inactive_file 268435456\n")
	write(t, root, "cpu/cpu.cfs_quota_us", "150000")
	write(t, root, "cpu/cpu.cfs_period_us", "100000")
	m := &Meter{Root: root, Cores: func() float64 { return 8 }}
	if m.cores() != 1.5 {
		t.Fatal(m.cores())
	}
	used, total := m.ram()
	if *total != 2<<30 || *used != (1<<30)-(256<<20) {
		t.Fatalf("got %d of %d", *used, *total)
	}
}

func TestAmdSysfsSkipsOtherVendorsAndConnectors(t *testing.T) {
	drm := t.TempDir()
	write(t, drm, "card3/device/vendor", "0x1002\n")
	write(t, drm, "card3/device/gpu_busy_percent", "42\n")
	write(t, drm, "card3/device/mem_info_vram_used", "1024\n")
	write(t, drm, "card3/device/mem_info_vram_total", "4096\n")
	write(t, drm, "card1/device/vendor", "0x10de\n")
	write(t, drm, "card3-DP-1/device/vendor", "0x1002\n")
	found := amdSysfs(drm)
	if len(found) != 1 || found[0].Index != 3 || *found[0].GPUPct != 42 || *found[0].VRAMTotal != 4096 {
		t.Fatalf("%+v", found)
	}
}

func TestToolOutputsParse(t *testing.T) {
	nv := ParseNvidiaSMI([]byte("37, 1024, 49140\n[N/A], 10, 20\n"))
	if len(nv) != 2 || *nv[0].GPUPct != 37 || *nv[0].VRAMUsed != 1024<<20 || nv[1].GPUPct != nil {
		t.Fatalf("%+v", nv)
	}
	rocm, err := ParseRocmSMI([]byte(`{"card0": {"GPU use (%)": "7", "VRAM Total Memory (B)": "17179869184", "VRAM Total Used Memory (B)": "8589934592"}, "system": {"x": 1}}`))
	if err != nil || len(rocm) != 1 || *rocm[0].GPUPct != 7 || *rocm[0].VRAMUsed != 8<<30 {
		t.Fatalf("%+v %v", rocm, err)
	}
	smi, err := ParseAmdSMI([]byte(`[{"gpu": 1, "usage": {"gfx_activity": {"value": 15, "unit": "%"}}, "mem_usage": {"used_vram": {"value": 1024, "unit": "MB"}, "total_vram": {"value": 8192, "unit": "MB"}}}]`))
	if err != nil || smi[0].Index != 1 || *smi[0].GPUPct != 15 || *smi[0].VRAMUsed != 1<<30 {
		t.Fatalf("%+v %v", smi, err)
	}
	wrapped, err := ParseAmdSMI([]byte(`{"gpu_data": [{"gpu": 0, "usage": {"gfx_activity": {"value": "N/A"}}}]}`))
	if err != nil || len(wrapped) != 1 || wrapped[0].GPUPct != nil {
		t.Fatalf("%+v %v", wrapped, err)
	}
}

func TestAMinuteBecomesOneReport(t *testing.T) {
	f := func(v float64) *float64 { return &v }
	u := func(v uint64) *uint64 { return &v }
	hundred := u(100)
	readings := []Reading{
		{RAMPct: f(10), RAMUsed: u(1), GPUs: []GPU{{Vendor: "nvidia", GPUPct: f(20), VRAMUsed: u(10), VRAMTotal: hundred}}},
		{CPUPct: f(30), RAMPct: f(20), RAMUsed: u(2), GPUs: []GPU{{Vendor: "nvidia", GPUPct: f(80), VRAMUsed: u(30), VRAMTotal: hundred}}},
		{CPUPct: f(90), RAMPct: f(30), RAMUsed: u(3), GPUs: []GPU{{Vendor: "nvidia", VRAMUsed: u(50), VRAMTotal: hundred}}},
	}
	got := Combine(readings)
	if got.Samples != 3 || *got.CPUPct != 60 || *got.CPUPeak != 90 || *got.RAMPct != 20 || *got.RAMUsed != 3 {
		t.Fatalf("%+v", got)
	}
	if *got.GPUPct != 50 || *got.GPUPeak != 80 || *got.VRAMPct != 30 || *got.VRAMUsed != 50 || got.GPUs != 1 {
		t.Fatalf("%+v", got)
	}
	if empty := Combine(nil); empty.CPUPct != nil || empty.GPUs != 0 {
		t.Fatalf("%+v", empty)
	}
}

func TestThisMachineSamples(t *testing.T) {
	m := New()
	m.Sample(context.Background())
	reading := m.Sample(context.Background())
	encoded, _ := json.Marshal(Combine([]Reading{reading}))
	t.Log(string(encoded))
	if reading.RAMTotal != nil && *reading.RAMTotal == 0 {
		t.Fatal("zero memory")
	}
	if reading.CPUPct != nil && (*reading.CPUPct < 0 || *reading.CPUPct > 100) {
		t.Fatal(*reading.CPUPct)
	}
}

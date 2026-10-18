// Package usage samples CPU, RAM, GPU, and VRAM utilization and reports it once a minute.
//
// Inside a container the numbers are the container's own: CPU against its
// quota or allowed cores, RAM against its memory limit without reclaimable
// page cache. NVIDIA is read with nvidia-smi, which the NVIDIA container
// runtime mounts in. AMD is read from amdgpu sysfs, then amd-smi, then rocm-smi.
// Anything that cannot be read is left out, never guessed.
package usage

import (
	"context"
	"encoding/json"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	// Sample is how often the meter reads. Report is how often an aggregate is sent.
	Sample = 10 * time.Second
	Report = 60 * time.Second
	amd    = "0x1002"
)

// Reading is one sample.
type Reading struct {
	CPUPct   *float64
	RAMPct   *float64
	RAMUsed  *uint64
	RAMTotal *uint64
	GPUs     []GPU
}

// GPU is one device in a sample.
type GPU struct {
	Vendor    string
	Index     int
	GPUPct    *float64
	VRAMUsed  *uint64
	VRAMTotal *uint64
}

// Aggregate is what the control plane stores for a minute of samples.
type Aggregate struct {
	Samples   int      `json:"samples"`
	CPUPct    *float64 `json:"cpu_pct"`
	CPUPeak   *float64 `json:"cpu_peak"`
	RAMPct    *float64 `json:"ram_pct"`
	RAMUsed   *uint64  `json:"ram_used"`
	RAMTotal  *uint64  `json:"ram_total"`
	GPUPct    *float64 `json:"gpu_pct"`
	GPUPeak   *float64 `json:"gpu_peak"`
	VRAMPct   *float64 `json:"vram_pct"`
	VRAMUsed  *uint64  `json:"vram_used"`
	VRAMTotal *uint64  `json:"vram_total"`
	GPUs      int      `json:"gpus"`
}

// Meter keeps the previous CPU reading. Root is the cgroup mount, Drm the DRM class folder.
type Meter struct {
	Root  string
	Drm   string
	Cores func() float64
	Run   func(ctx context.Context, name string, args ...string) ([]byte, error)
	Now   func() time.Time

	mark time.Time
	busy float64
	have bool
}

// New reads the real system.
func New() *Meter {
	return &Meter{Root: "/sys/fs/cgroup", Drm: "/sys/class/drm"}
}

// Sample reads everything once. The first call has no CPU value because CPU is a delta.
func (m *Meter) Sample(ctx context.Context) Reading {
	used, total := m.ram()
	reading := Reading{CPUPct: m.cpu(), RAMUsed: used, RAMTotal: total, RAMPct: pct(used, total)}
	reading.GPUs = m.nvidia(ctx)
	reading.GPUs = append(reading.GPUs, amdSysfs(m.Drm)...)
	if !hasVendor(reading.GPUs, "amd") {
		reading.GPUs = append(reading.GPUs, m.amdTools(ctx)...)
	}
	return reading
}

func (m *Meter) now() time.Time {
	if m.Now != nil {
		return m.Now()
	}
	return time.Now()
}

func (m *Meter) cpu() *float64 {
	busy, ok := m.cpuSeconds()
	if !ok {
		return nil
	}
	now := m.now()
	last, lastBusy, had := m.mark, m.busy, m.have
	m.mark, m.busy, m.have = now, busy, true
	elapsed := now.Sub(last).Seconds()
	if !had || elapsed <= 0 {
		return nil
	}
	return clamp((busy - lastBusy) / (elapsed * m.cores()) * 100)
}

func (m *Meter) cpuSeconds() (float64, bool) {
	if folder := cgroup(m.Root); folder != "" {
		if value, ok := statValue(filepath.Join(folder, "cpu.stat"), "usage_usec"); ok {
			return float64(value) / 1e6, true
		}
	}
	for _, sub := range []string{"cpuacct", "cpu,cpuacct"} {
		if value, ok := readUint(filepath.Join(m.Root, sub, "cpuacct.usage")); ok {
			return float64(value) / 1e9, true
		}
	}
	text, err := os.ReadFile("/proc/stat")
	if err != nil {
		return 0, false
	}
	line, _, _ := strings.Cut(string(text), "\n")
	fields := strings.Fields(line)
	if len(fields) < 5 || fields[0] != "cpu" {
		return 0, false
	}
	var total, idle float64
	for i, field := range fields[1:] {
		value, err := strconv.ParseFloat(field, 64)
		if err != nil {
			return 0, false
		}
		total += value
		if i == 3 || i == 4 {
			idle += value
		}
	}
	// USER_HZ is 100 on every Linux build Surfra runs on.
	return (total - idle) / 100, true
}

func (m *Meter) cores() float64 {
	allowed := float64(runtime.NumCPU())
	if m.Cores != nil {
		allowed = m.Cores()
	}
	quota := 0.0
	if folder := cgroup(m.Root); folder != "" {
		fields := strings.Fields(readString(filepath.Join(folder, "cpu.max")))
		if len(fields) == 2 && fields[0] != "max" {
			limit, err1 := strconv.ParseFloat(fields[0], 64)
			period, err2 := strconv.ParseFloat(fields[1], 64)
			if err1 == nil && err2 == nil && period > 0 {
				quota = limit / period
			}
		}
	} else {
		for _, sub := range []string{"cpu", "cpu,cpuacct"} {
			limit, err1 := strconv.ParseFloat(readString(filepath.Join(m.Root, sub, "cpu.cfs_quota_us")), 64)
			period, err2 := strconv.ParseFloat(readString(filepath.Join(m.Root, sub, "cpu.cfs_period_us")), 64)
			if err1 == nil && err2 == nil && limit > 0 && period > 0 {
				quota = limit / period
				break
			}
		}
	}
	if quota > 0 && quota < allowed {
		allowed = quota
	}
	return math.Max(allowed, 0.01)
}

func (m *Meter) ram() (*uint64, *uint64) {
	hostUsed, hostTotal, hostOK := meminfo()
	if folder := cgroup(m.Root); folder != "" {
		if current, ok := readUint(filepath.Join(folder, "memory.current")); ok {
			inactive, _ := statValue(filepath.Join(folder, "memory.stat"), "inactive_file")
			used := sub(current, inactive)
			if limit, ok := readUint(filepath.Join(folder, "memory.max")); ok {
				return &used, &limit
			}
			if hostOK {
				return &used, &hostTotal
			}
		}
	}
	limit, ok1 := readUint(filepath.Join(m.Root, "memory", "memory.limit_in_bytes"))
	current, ok2 := readUint(filepath.Join(m.Root, "memory", "memory.usage_in_bytes"))
	if ok1 && ok2 && hostOK && limit < hostTotal {
		inactive, _ := statValue(filepath.Join(m.Root, "memory", "memory.stat"), "total_inactive_file")
		used := sub(current, inactive)
		return &used, &limit
	}
	if !hostOK {
		return nil, nil
	}
	return &hostUsed, &hostTotal
}

// cgroup is this process's cgroup v2 folder. In a cgroup namespace /proc/self/cgroup says "/",
// and the mount root is the container's own cgroup.
func cgroup(root string) string {
	if _, err := os.Stat(filepath.Join(root, "cgroup.controllers")); err != nil {
		return ""
	}
	for _, line := range strings.Split(readString("/proc/self/cgroup"), "\n") {
		path, ok := strings.CutPrefix(line, "0::")
		if !ok || path == "" || path == "/" {
			continue
		}
		folder := filepath.Join(root, path)
		if _, err := os.Stat(filepath.Join(folder, "cpu.stat")); err == nil {
			return folder
		}
	}
	if _, err := os.Stat(filepath.Join(root, "cpu.stat")); err == nil {
		return root
	}
	return ""
}

func meminfo() (uint64, uint64, bool) {
	info := map[string]uint64{}
	for _, line := range strings.Split(readString("/proc/meminfo"), "\n") {
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		if value, err := strconv.ParseUint(fields[1], 10, 64); err == nil {
			info[strings.TrimSuffix(fields[0], ":")] = value * 1024
		}
	}
	total, ok1 := info["MemTotal"]
	available, ok2 := info["MemAvailable"]
	if !ok1 || !ok2 {
		return 0, 0, false
	}
	return sub(total, available), total, true
}

func (m *Meter) run(ctx context.Context, name string, args ...string) ([]byte, error) {
	if m.Run != nil {
		return m.Run(ctx, name, args...)
	}
	path, err := exec.LookPath(name)
	if err != nil {
		// A FROM scratch image has no PATH. The NVIDIA runtime mounts its tools here.
		path = "/usr/bin/" + name
		if _, err := os.Stat(path); err != nil {
			return nil, err
		}
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	return exec.CommandContext(ctx, path, args...).Output()
}

func (m *Meter) nvidia(ctx context.Context) []GPU {
	out, err := m.run(ctx, "nvidia-smi", "--query-gpu=utilization.gpu,memory.used,memory.total", "--format=csv,noheader,nounits")
	if err != nil {
		return nil
	}
	return ParseNvidiaSMI(out)
}

// ParseNvidiaSMI reads the csv,noheader,nounits form.
func ParseNvidiaSMI(out []byte) []GPU {
	var found []GPU
	for index, line := range strings.Split(strings.TrimSpace(string(out)), "\n") {
		cells := strings.Split(line, ",")
		if len(cells) != 3 {
			continue
		}
		found = append(found, GPU{Vendor: "nvidia", Index: index, GPUPct: percent(cells[0]), VRAMUsed: mib(cells[1]), VRAMTotal: mib(cells[2])})
	}
	return found
}

func amdSysfs(root string) []GPU {
	matches, _ := filepath.Glob(filepath.Join(root, "card[0-9]*"))
	sort.Strings(matches)
	var found []GPU
	for _, card := range matches {
		number, err := strconv.Atoi(strings.TrimPrefix(filepath.Base(card), "card"))
		if err != nil {
			continue
		}
		device := filepath.Join(card, "device")
		if strings.ToLower(readString(filepath.Join(device, "vendor"))) != amd {
			continue
		}
		used, ok1 := readUint(filepath.Join(device, "mem_info_vram_used"))
		total, ok2 := readUint(filepath.Join(device, "mem_info_vram_total"))
		if !ok1 || !ok2 {
			continue
		}
		found = append(found, GPU{Vendor: "amd", Index: number, GPUPct: percent(readString(filepath.Join(device, "gpu_busy_percent"))), VRAMUsed: &used, VRAMTotal: &total})
	}
	return found
}

func (m *Meter) amdTools(ctx context.Context) []GPU {
	if out, err := m.run(ctx, "amd-smi", "metric", "--json"); err == nil {
		if found, err := ParseAmdSMI(out); err == nil {
			return found
		}
	}
	if out, err := m.run(ctx, "rocm-smi", "--showuse", "--showmeminfo", "vram", "--json"); err == nil {
		if found, err := ParseRocmSMI(out); err == nil {
			return found
		}
	}
	return nil
}

// ParseRocmSMI reads rocm-smi --showuse --showmeminfo vram --json.
func ParseRocmSMI(raw []byte) ([]GPU, error) {
	var payload map[string]json.RawMessage
	if err := json.Unmarshal(raw, &payload); err != nil {
		return nil, err
	}
	names := make([]string, 0, len(payload))
	for name := range payload {
		names = append(names, name)
	}
	sort.Strings(names)
	var found []GPU
	for _, name := range names {
		index, err := strconv.Atoi(strings.TrimPrefix(name, "card"))
		if !strings.HasPrefix(name, "card") || err != nil {
			continue
		}
		var info map[string]any
		if json.Unmarshal(payload[name], &info) != nil {
			continue
		}
		found = append(found, GPU{
			Vendor:    "amd",
			Index:     index,
			GPUPct:    first(info, clampPtr, "GPU use (%)", "GPU Utilization", "GPU use"),
			VRAMUsed:  wholeField(info, "VRAM Total Used Memory (B)", "VRAM Used Memory (B)"),
			VRAMTotal: wholeField(info, "VRAM Total Memory (B)", "VRAM Total Memory"),
		})
	}
	return found, nil
}

// ParseAmdSMI reads amd-smi metric --json, a list or an object with a gpu_data or gpu list.
func ParseAmdSMI(raw []byte) ([]GPU, error) {
	var rows []map[string]any
	if err := json.Unmarshal(raw, &rows); err != nil {
		var wrapped struct {
			Data []map[string]any `json:"gpu_data"`
			GPU  []map[string]any `json:"gpu"`
		}
		if err2 := json.Unmarshal(raw, &wrapped); err2 != nil {
			return nil, err
		}
		rows = wrapped.Data
		if rows == nil {
			rows = wrapped.GPU
		}
	}
	found := make([]GPU, 0, len(rows))
	for _, row := range rows {
		index := len(found)
		if value, ok := number(row["gpu"]); ok {
			index = int(value)
		} else if value, ok := number(row["id"]); ok {
			index = int(value)
		}
		use, _ := row["usage"].(map[string]any)
		activity, _ := use["gfx_activity"].(map[string]any)
		memory, _ := row["mem_usage"].(map[string]any)
		if memory == nil {
			memory, _ = row["vram_usage"].(map[string]any)
		}
		var gpuPct *float64
		if value, ok := number(activity["value"]); ok {
			gpuPct = clamp(value)
		}
		found = append(found, GPU{
			Vendor:    "amd",
			Index:     index,
			GPUPct:    gpuPct,
			VRAMUsed:  scaled(node(memory, "used_vram", "vram_used", "used")),
			VRAMTotal: scaled(node(memory, "total_vram", "vram_total", "total")),
		})
	}
	return found, nil
}

// Combine turns a minute of readings into one report.
func Combine(readings []Reading) Aggregate {
	out := Aggregate{Samples: len(readings)}
	var cpu, ram, gpu, vram, peaks []float64
	for _, reading := range readings {
		if reading.CPUPct != nil {
			cpu = append(cpu, *reading.CPUPct)
		}
		if reading.RAMPct != nil {
			ram = append(ram, *reading.RAMPct)
		}
		var each []float64
		var used, total uint64
		for _, device := range reading.GPUs {
			if device.GPUPct != nil {
				each = append(each, *device.GPUPct)
				peaks = append(peaks, *device.GPUPct)
			}
			if device.VRAMUsed != nil && device.VRAMTotal != nil && *device.VRAMTotal > 0 {
				used += *device.VRAMUsed
				total += *device.VRAMTotal
			}
		}
		if len(each) > 0 {
			gpu = append(gpu, *mean(each))
		}
		if total > 0 {
			vram = append(vram, float64(used)/float64(total)*100)
		}
	}
	out.CPUPct, out.CPUPeak = mean(cpu), peak(cpu)
	out.RAMPct = mean(ram)
	out.GPUPct, out.GPUPeak = mean(gpu), peak(peaks)
	out.VRAMPct = mean(vram)
	if len(readings) > 0 {
		last := readings[len(readings)-1]
		out.RAMUsed, out.RAMTotal = last.RAMUsed, last.RAMTotal
		out.GPUs = len(last.GPUs)
		var used, total uint64
		for _, device := range last.GPUs {
			if device.VRAMUsed != nil && device.VRAMTotal != nil && *device.VRAMTotal > 0 {
				used += *device.VRAMUsed
				total += *device.VRAMTotal
			}
		}
		if total > 0 {
			out.VRAMUsed, out.VRAMTotal = &used, &total
		}
	}
	return out
}

// Loop samples every Sample and hands send an aggregate every Report until ctx ends.
func (m *Meter) Loop(ctx context.Context, send func(context.Context, Aggregate)) {
	m.Sample(ctx)
	ticker := time.NewTicker(Sample)
	defer ticker.Stop()
	var readings []Reading
	sent := time.Now()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		readings = append(readings, m.Sample(ctx))
		if time.Since(sent) < Report {
			continue
		}
		send(ctx, Combine(readings))
		readings, sent = nil, time.Now()
	}
}

func node(memory map[string]any, keys ...string) map[string]any {
	for _, key := range keys {
		if value, ok := memory[key].(map[string]any); ok {
			return value
		}
	}
	return nil
}

func first(info map[string]any, convert func(float64) *float64, keys ...string) *float64 {
	for _, key := range keys {
		if value, ok := number(info[key]); ok {
			return convert(value)
		}
	}
	return nil
}

func wholeField(info map[string]any, keys ...string) *uint64 {
	for _, key := range keys {
		if value, ok := number(info[key]); ok && value >= 0 {
			out := uint64(value)
			return &out
		}
	}
	return nil
}

func number(value any) (float64, bool) {
	switch typed := value.(type) {
	case float64:
		return typed, !math.IsNaN(typed) && !math.IsInf(typed, 0)
	case string:
		parsed, err := strconv.ParseFloat(strings.TrimSuffix(strings.TrimSpace(typed), "%"), 64)
		return parsed, err == nil && !math.IsNaN(parsed) && !math.IsInf(parsed, 0)
	default:
		return 0, false
	}
}

func scaled(n map[string]any) *uint64 {
	value, ok := number(n["value"])
	if !ok || value < 0 {
		return nil
	}
	unit, _ := n["unit"].(string)
	factor := map[string]float64{"kb": 1024, "mb": 1 << 20, "gb": 1 << 30, "kib": 1024, "mib": 1 << 20, "gib": 1 << 30}[strings.ToLower(unit)]
	if factor == 0 {
		factor = 1
	}
	out := uint64(value * factor)
	return &out
}

func percent(text string) *float64 {
	value, ok := number(text)
	if !ok {
		return nil
	}
	return clamp(value)
}

func mib(text string) *uint64 {
	value, ok := number(text)
	if !ok || value < 0 {
		return nil
	}
	out := uint64(value) << 20
	return &out
}

func readString(path string) string {
	data, err := os.ReadFile(path)
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(data))
}

func readUint(path string) (uint64, bool) {
	value, err := strconv.ParseUint(readString(path), 10, 64)
	return value, err == nil
}

func statValue(path, key string) (uint64, bool) {
	for _, line := range strings.Split(readString(path), "\n") {
		name, value, ok := strings.Cut(line, " ")
		if ok && name == key {
			parsed, err := strconv.ParseUint(strings.TrimSpace(value), 10, 64)
			return parsed, err == nil
		}
	}
	return 0, false
}

func sub(a, b uint64) uint64 {
	if b > a {
		return 0
	}
	return a - b
}

func pct(used, total *uint64) *float64 {
	if used == nil || total == nil || *total == 0 {
		return nil
	}
	return clamp(float64(*used) / float64(*total) * 100)
}

func clampPtr(value float64) *float64 { return clamp(value) }

func clamp(value float64) *float64 {
	value = math.Round(math.Min(100, math.Max(0, value))*10) / 10
	return &value
}

func mean(values []float64) *float64 {
	if len(values) == 0 {
		return nil
	}
	total := 0.0
	for _, value := range values {
		total += value
	}
	out := math.Round(total/float64(len(values))*10) / 10
	return &out
}

func peak(values []float64) *float64 {
	if len(values) == 0 {
		return nil
	}
	out := values[0]
	for _, value := range values[1:] {
		out = math.Max(out, value)
	}
	return &out
}

func hasVendor(gpus []GPU, vendor string) bool {
	for _, device := range gpus {
		if device.Vendor == vendor {
			return true
		}
	}
	return false
}

package main

import (
	"fmt"
	"runtime"
	"sync"
	"syscall"
	"time"
)

type ResourceMonitor struct {
	samples      []ResourceSample
	startTime    time.Time // fixed start time of monitoring
	oldStartTime time.Time
	oldCPUTime   float64
	ticker       *time.Ticker
	done         chan interface{}
	mu           sync.Mutex
}

func (m *ResourceMonitor) Start(interval time.Duration) func() {
	m.startTime = time.Now()
	m.oldStartTime = m.startTime
	m.samples = []ResourceSample{}
	m.oldCPUTime = m.getCPUTime()
	m.ticker = time.NewTicker(interval)
	m.done = make(chan interface{})

	return func() {
		for {
			select {
			case <-m.ticker.C:
				m.RecordSample()
			case <-m.done:
				return
			}
		}
	}
}

func (m *ResourceMonitor) Stop() *ResourceStats {
	m.ticker.Stop()
	close(m.done)

	m.mu.Lock()
	defer m.mu.Unlock()

	n := len(m.samples)
	if n == 0 {
		return &ResourceStats{}
	}

	var cpuMean, cpuMax float64
	var memMean float64
	var memMax int64
	for _, s := range m.samples {
		cpuMean += s.cpuPercent / float64(n)
		if s.cpuPercent > cpuMax {
			cpuMax = s.cpuPercent
		}
		memMean += float64(s.memoryBytes) / float64(n)
		if s.memoryBytes > memMax {
			memMax = s.memoryBytes
		}
	}

	return &ResourceStats{
		cpuPercentMean:  cpuMean,
		cpuPercentMax:   cpuMax,
		memoryBytesMean: memMean,
		memoryBytesMax:  memMax,
		sampleCount:     n,
	}
}

func (m *ResourceMonitor) RecordSample() {
	m.mu.Lock()
	defer m.mu.Unlock()
	cpuUsage := m.GetCPUUsage()
	var mstat runtime.MemStats
	runtime.ReadMemStats(&mstat)
	m.samples = append(m.samples, ResourceSample{
		cpuPercent:  cpuUsage,
		memoryBytes: int64(mstat.Alloc),
	})
}

func (m *ResourceMonitor) GetCPUUsage() float64 {
	endCPU := m.getCPUTime()
	endTime := time.Now()

	elapsed := endTime.Sub(m.oldStartTime).Seconds()
	if elapsed < 0.001 {
		m.oldCPUTime = endCPU
		m.oldStartTime = endTime
		return 0
	}

	cpuUsage := (endCPU - m.oldCPUTime) / elapsed * 100
	m.oldCPUTime = endCPU
	m.oldStartTime = endTime
	return cpuUsage
}

func (m *ResourceMonitor) getCPUTime() float64 {
	var usage syscall.Rusage
	syscall.Getrusage(syscall.RUSAGE_SELF, &usage)
	user := float64(usage.Utime.Sec) + float64(usage.Utime.Usec)/1e6
	sys := float64(usage.Stime.Sec) + float64(usage.Stime.Usec)/1e6
	return user + sys
}

type ResourceSample struct {
	cpuPercent  float64
	memoryBytes int64
}

type ResourceStats struct {
	cpuPercentMean  float64
	cpuPercentMax   float64
	memoryBytesMean float64
	memoryBytesMax  int64
	sampleCount     int
}

func (s *ResourceStats) Print() {
	fmt.Println("\n=== Resource Usage Statistics ===")
	fmt.Printf("Samples collected: %d\n", s.sampleCount)
	fmt.Println("\nCPU Usage:")
	fmt.Printf("  Mean: %.1f%%\n", s.cpuPercentMean)
	fmt.Printf("  Max:  %.1f%%\n", s.cpuPercentMax)
	fmt.Println("\nMemory Usage:")
	fmt.Printf("  Mean: %.1f MB\n", s.memoryBytesMean/1024/1024)
	fmt.Printf("  Max:  %.1f MB\n", float64(s.memoryBytesMax)/1024/1024)
	fmt.Println("=================================")
}

func (s *ResourceStats) PrintCompact(prefix string) {
	fmt.Printf("%sCPU mean: %.1f%% (max: %.1f%%), Memory mean: %.1f MB (max: %.1f MB)\n",
		prefix,
		s.cpuPercentMean,
		s.cpuPercentMax,
		s.memoryBytesMean/1024/1024,
		float64(s.memoryBytesMax)/1024/1024)
}

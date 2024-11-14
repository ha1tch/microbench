// Package metrics provides metrics collection for benchmarking tools
package metrics

import (
    "sort"
    "sync"
    "time"
)

// PatternMetrics tracks pattern-specific performance data
type PatternMetrics struct {
    TotalTransactions      int64
    SuccessfulTransactions int64
    FailedTransactions     int64
    Rollbacks             int64
    PhaseLatencies        map[string][]time.Duration
    ResourceUsage         map[string]int64
    ErrorsByType          map[string]int64
}

// Collector gathers performance metrics
type Collector struct {
    mu sync.RWMutex

    // Overall metrics
    startTime     time.Time
    endTime       time.Time
    messagesSent  int64
    messagesRecv  int64

    // Pattern-specific metrics
    patterns map[string]*PatternMetrics

    // Raw latency data for percentile calculations
    latencies []time.Duration
}

// NewCollector creates a new metrics collector
func NewCollector() *Collector {
    return &Collector{
        startTime: time.Now(),
        patterns:  make(map[string]*PatternMetrics),
        latencies: make([]time.Duration, 0),
    }
}

// RecordTransaction records transaction metrics for a specific pattern
func (c *Collector) RecordTransaction(pattern string, duration time.Duration, successful bool, phases map[string]time.Duration) {
    c.mu.Lock()
    defer c.mu.Unlock()

    metrics, exists := c.patterns[pattern]
    if !exists {
        metrics = &PatternMetrics{
            PhaseLatencies: make(map[string][]time.Duration),
            ResourceUsage:  make(map[string]int64),
            ErrorsByType:   make(map[string]int64),
        }
        c.patterns[pattern] = metrics
    }

    // Update transaction counts
    metrics.TotalTransactions++
    if successful {
        metrics.SuccessfulTransactions++
    } else {
        metrics.FailedTransactions++
    }

    // Record overall latency
    c.latencies = append(c.latencies, duration)

    // Record phase latencies
    for phase, latency := range phases {
        if _, exists := metrics.PhaseLatencies[phase]; !exists {
            metrics.PhaseLatencies[phase] = make([]time.Duration, 0)
        }
        metrics.PhaseLatencies[phase] = append(metrics.PhaseLatencies[phase], latency)
    }
}

// RecordRollback tracks rollback operations
func (c *Collector) RecordRollback(pattern string) {
    c.mu.Lock()
    defer c.mu.Unlock()

    if metrics, exists := c.patterns[pattern]; exists {
        metrics.Rollbacks++
    }
}

// RecordResourceUsage tracks resource utilization
func (c *Collector) RecordResourceUsage(pattern, resource string, count int64) {
    c.mu.Lock()
    defer c.mu.Unlock()

    if metrics, exists := c.patterns[pattern]; exists {
        metrics.ResourceUsage[resource] += count
    }
}

// RecordError tracks error occurrences by type
func (c *Collector) RecordError(pattern, errorType string) {
    c.mu.Lock()
    defer c.mu.Unlock()

    if metrics, exists := c.patterns[pattern]; exists {
        metrics.ErrorsByType[errorType]++
    }
}

// RecordMessage tracks message counts
func (c *Collector) RecordMessage(sent bool) {
    c.mu.Lock()
    defer c.mu.Unlock()

    if sent {
        c.messagesSent++
    } else {
        c.messagesRecv++
    }
}

// GetPatternStats returns statistics for a specific pattern
func (c *Collector) GetPatternStats(pattern string) *PatternMetrics {
    c.mu.RLock()
    defer c.mu.RUnlock()

    if metrics, exists := c.patterns[pattern]; exists {
        // Return a copy to avoid concurrent access issues
        copy := &PatternMetrics{
            TotalTransactions:      metrics.TotalTransactions,
            SuccessfulTransactions: metrics.SuccessfulTransactions,
            FailedTransactions:     metrics.FailedTransactions,
            Rollbacks:             metrics.Rollbacks,
            PhaseLatencies:        make(map[string][]time.Duration),
            ResourceUsage:         make(map[string]int64),
            ErrorsByType:          make(map[string]int64),
        }

        // Copy phase latencies
        for phase, latencies := range metrics.PhaseLatencies {
            copy.PhaseLatencies[phase] = make([]time.Duration, len(latencies))
            copy.PhaseLatencies[phase] = append(copy.PhaseLatencies[phase], latencies...)
        }

        // Copy resource usage
        for resource, count := range metrics.ResourceUsage {
            copy.ResourceUsage[resource] = count
        }

        // Copy error counts
        for errType, count := range metrics.ErrorsByType {
            copy.ErrorsByType[errType] = count
        }

        return copy
    }
    return nil
}

// GetOverallStats returns general performance statistics
func (c *Collector) GetOverallStats() map[string]interface{} {
    c.mu.RLock()
    defer c.mu.RUnlock()

    stats := make(map[string]interface{})

    // Duration
    stats["duration"] = time.Since(c.startTime)

    // Message counts
    stats["messages_sent"] = c.messagesSent
    stats["messages_received"] = c.messagesRecv

    // Calculate percentiles if we have latency data
    if len(c.latencies) > 0 {
        sorted := make([]time.Duration, len(c.latencies))
        copy(sorted, c.latencies)
        sort.Slice(sorted, func(i, j int) bool {
            return sorted[i] < sorted[j]
        })

        stats["latency_p50"] = sorted[len(sorted)/2]
        stats["latency_p95"] = sorted[int(float64(len(sorted))*0.95)]
        stats["latency_p99"] = sorted[int(float64(len(sorted))*0.99)]

        var sum time.Duration
        for _, d := range sorted {
            sum += d
        }
        stats["latency_avg"] = sum / time.Duration(len(sorted))
    }

    return stats
}

// Reset clears all collected metrics
func (c *Collector) Reset() {
    c.mu.Lock()
    defer c.mu.Unlock()

    c.startTime = time.Now()
    c.messagesSent = 0
    c.messagesRecv = 0
    c.patterns = make(map[string]*PatternMetrics)
    c.latencies = make([]time.Duration, 0)
}

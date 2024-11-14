//------------------------------------------------------------------------------
// PART 1: Core structures and result tracking
//------------------------------------------------------------------------------

package txbench

import (
	"context"
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ha1tch/microbench/internal/broker"
	"github.com/ha1tch/microbench/internal/patterns"
	"github.com/ha1tch/microbench/internal/services"
	cs "github.com/ha1tch/microbench/internal/services/core"
)

// Config holds benchmark configuration
type Config struct {
	// Broker for messaging
	Broker broker.Broker

	// Concurrency level (number of parallel transactions)
	Concurrency int

	// FailureRate for simulated failures (0.0-1.0)
	FailureRate float64

	// Timeout for transactions
	Timeout time.Duration

	// WarmupDuration before collecting metrics
	WarmupDuration time.Duration

	// ResourceInitCount is the initial number of resources to create
	ResourceInitCount int
}

// Results holds benchmark results
type Results struct {
	// Transaction counts
	TotalTransactions      int64
	SuccessfulTransactions int64
	FailedTransactions     int64
	WarmupTransactions     int64

	// Latency metrics
	AverageLatency time.Duration
	MedianLatency  time.Duration
	P95Latency     time.Duration
	P99Latency     time.Duration

	// Resource usage
	MessageCount int64
	RetryCount   int64

	// Per-phase latency metrics
	PhaseLatencies map[string]time.Duration

	// Error counts by type
	ErrorsByType map[string]int64

	// Raw data for analysis
	latencies []time.Duration
	errors    []error

	// Start and end times
	StartTime time.Time
	EndTime   time.Time
}

// Benchmark manages transaction pattern testing
type Benchmark struct {
	config  *Config
	pattern patterns.Pattern

	// Services
	orderService     services.Service
	inventoryService services.Service
	paymentService   services.Service

	// Metrics
	mu             sync.RWMutex
	results        *Results
	inWarmup       bool
	warmupComplete bool
	resourcePool   *resourcePool
}

// resourcePool manages test resources
type resourcePool struct {
	mu sync.RWMutex

	orders    map[string]bool
	inventory map[string]bool
	payments  map[string]bool

	orderIndex     int64
	inventoryIndex int64
	paymentIndex   int64
}

// NewBenchmark creates a new benchmark instance
func NewBenchmark(config *Config, pattern patterns.Pattern) *Benchmark {
	if config.ResourceInitCount == 0 {
		config.ResourceInitCount = config.Concurrency * 10 // Default to 10x concurrency
	}

	return &Benchmark{
		config:  config,
		pattern: pattern,
		results: &Results{
			PhaseLatencies: make(map[string]time.Duration),
			ErrorsByType:   make(map[string]int64),
			latencies:      make([]time.Duration, 0, 1000), // Pre-allocate for better performance
			errors:         make([]error, 0),
			StartTime:      time.Now(),
		},
		resourcePool: &resourcePool{
			orders:    make(map[string]bool),
			inventory: make(map[string]bool),
			payments:  make(map[string]bool),
		},
	}
}

// initializeServices sets up the services for testing
func (b *Benchmark) initializeServices(ctx context.Context) error {
	// Create service configurations with retry policies
	orderConfig := &services.Config{
		ServiceType:   services.ServiceOrder,
		BrokerConfig:  b.config.Broker,
		RetryAttempts: 3,
		RetryDelay:    100 * time.Millisecond,
	}

	inventoryConfig := &services.Config{
		ServiceType:   services.ServiceInventory,
		BrokerConfig:  b.config.Broker,
		RetryAttempts: 3,
		RetryDelay:    100 * time.Millisecond,
	}

	paymentConfig := &services.Config{
		ServiceType:   services.ServicePayment,
		BrokerConfig:  b.config.Broker,
		RetryAttempts: 3,
		RetryDelay:    100 * time.Millisecond,
	}

	// Initialize services with error handling
	var err error
	if b.orderService, err = cs.NewOrderService(orderConfig); err != nil {
		return err
	}
	if err = b.orderService.Initialize(ctx); err != nil {
		return err
	}

	if b.inventoryService, err = cs.NewInventoryService(inventoryConfig); err != nil {
		return err
	}
	if err = b.inventoryService.Initialize(ctx); err != nil {
		return err
	}

	if b.paymentService, err = cs.NewPaymentService(paymentConfig); err != nil {
		return err
	}
	if err = b.paymentService.Initialize(ctx); err != nil {
		return err
	}

	// Pre-allocate test resources
	return b.initializeResources(ctx)
}

// initializeResources creates initial test resources
func (b *Benchmark) initializeResources(ctx context.Context) error {
	for i := 0; i < b.config.ResourceInitCount; i++ {
		orderId := fmt.Sprintf("order-%d", atomic.AddInt64(&b.resourcePool.orderIndex, 1))
		invId := fmt.Sprintf("inventory-%d", atomic.AddInt64(&b.resourcePool.inventoryIndex, 1))
		payId := fmt.Sprintf("payment-%d", atomic.AddInt64(&b.resourcePool.paymentIndex, 1))

		b.resourcePool.mu.Lock()
		b.resourcePool.orders[orderId] = true
		b.resourcePool.inventory[invId] = true
		b.resourcePool.payments[payId] = true
		b.resourcePool.mu.Unlock()
	}
	return nil
}

// getResources gets a set of resources for a transaction
func (b *Benchmark) getResources() (string, string, string) {
	b.resourcePool.mu.Lock()
	defer b.resourcePool.mu.Unlock()

	// Get random resources
	var orderId, invId, payId string
	for id := range b.resourcePool.orders {
		orderId = id
		break
	}
	for id := range b.resourcePool.inventory {
		invId = id
		break
	}
	for id := range b.resourcePool.payments {
		payId = id
		break
	}

	return orderId, invId, payId
}

// recordLatency adds a latency measurement
func (b *Benchmark) recordLatency(d time.Duration) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if !b.inWarmup {
		b.results.latencies = append(b.results.latencies, d)
	}
}

// recordError adds an error to the results
func (b *Benchmark) recordError(err error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if !b.inWarmup {
		b.results.errors = append(b.results.errors, err)
		if e, ok := err.(patterns.PatternError); ok {
			b.results.ErrorsByType[e.Code]++
		} else {
			b.results.ErrorsByType["unknown"]++
		}
	}
}

// incrementTransactionCount updates transaction counters
func (b *Benchmark) incrementTransactionCount(success bool) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.inWarmup {
		atomic.AddInt64(&b.results.WarmupTransactions, 1)
		return
	}

	atomic.AddInt64(&b.results.TotalTransactions, 1)
	if success {
		atomic.AddInt64(&b.results.SuccessfulTransactions, 1)
	} else {
		atomic.AddInt64(&b.results.FailedTransactions, 1)
	}
}

// incrementMessages increases message count
func (b *Benchmark) incrementMessages(count int) {
	if !b.inWarmup {
		atomic.AddInt64(&b.results.MessageCount, int64(count))
	}
}

// incrementRetries increases retry count
func (b *Benchmark) incrementRetries(count int) {
	if !b.inWarmup {
		atomic.AddInt64(&b.results.RetryCount, int64(count))
	}
}

// shouldFail determines if a simulated failure should occur
func (b *Benchmark) shouldFail() bool {
	return rand.Float64() < b.config.FailureRate
}

// calculateResults computes final statistics
func (b *Benchmark) calculateResults() {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.results.EndTime = time.Now()

	// Sort latencies for percentile calculations
	if len(b.results.latencies) > 0 {
		sort.Slice(b.results.latencies, func(i, j int) bool {
			return b.results.latencies[i] < b.results.latencies[j]
		})

		// Calculate statistics
		var total time.Duration
		for _, d := range b.results.latencies {
			total += d
		}

		b.results.AverageLatency = total / time.Duration(len(b.results.latencies))
		b.results.MedianLatency = b.results.latencies[len(b.results.latencies)/2]

		p95Index := int(float64(len(b.results.latencies)) * 0.95)
		b.results.P95Latency = b.results.latencies[p95Index]

		p99Index := int(float64(len(b.results.latencies)) * 0.99)
		b.results.P99Latency = b.results.latencies[p99Index]
	}
}

//------------------------------------------------------------------------------
// PART 2: Transaction execution and benchmarking logic
//------------------------------------------------------------------------------

package txbench

import (
	"context"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/ha1tch/microbench/internal/patterns"
	"github.com/ha1tch/microbench/internal/services"
)

// Run executes the benchmark
func (b *Benchmark) Run(ctx context.Context) (*Results, error) {
	// Initialize services
	if err := b.initializeServices(ctx); err != nil {
		return nil, fmt.Errorf("failed to initialize services: %v", err)
	}

	// Create worker pool
	var wg sync.WaitGroup
	workChan := make(chan struct{}, b.config.Concurrency)

	// Start workers
	for i := 0; i < b.config.Concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-ctx.Done():
					return
				case <-workChan:
					b.executeTransaction(ctx)
				}
			}
		}()
	}

	// Feed work to the pool
	ticker := time.NewTicker(10 * time.Millisecond) // Control transaction rate
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			close(workChan)
			wg.Wait()
			b.calculateResults()
			return b.results, nil
		case <-ticker.C:
			select {
			case workChan <- struct{}{}:
				// Work queued successfully
			default:
				// Queue full, skip this tick
			}
		}
	}
}

// executeTransaction runs a single test transaction
func (b *Benchmark) executeTransaction(ctx context.Context) {
	startTime := time.Now()
	txCtx, cancel := context.WithTimeout(ctx, b.config.Timeout)
	defer cancel()

	// Create test resources
	orderID := fmt.Sprintf("order-%d", time.Now().UnixNano())
	inventoryID := fmt.Sprintf("inventory-%d", time.Now().UnixNano())
	paymentID := fmt.Sprintf("payment-%d", time.Now().UnixNano())

	// Create participants
	participants := []*patterns.Participant{
		{
			Service:    b.orderService,
			ResourceID: orderID,
		},
		{
			Service:    b.inventoryService,
			ResourceID: inventoryID,
		},
		{
			Service:    b.paymentService,
			ResourceID: paymentID,
		},
	}

	// Inject failure if needed
	if b.shouldFail() {
		// Simulate a service failure
		participants[0].Service = &failingService{
			Service: participants[0].Service,
			phase:   randomFailurePhase(),
		}
	}

	// Execute transaction
	tx, err := b.pattern.Execute(txCtx, participants)

	// Record metrics
	duration := time.Since(startTime)
	b.recordLatency(duration)

	if err != nil {
		b.recordError(err)
	}

	// Record phase latencies if available
	if tx != nil {
		for phase, latency := range getPhaseLatencies(tx) {
			b.mu.Lock()
			b.results.PhaseLatencies[phase] = latency
			b.mu.Unlock()
		}
	}

	// Track message counts (approximate)
	msgCount := len(participants) * 2 // Minimum messages per participant
	if err != nil {
		msgCount *= 2 // Additional messages for rollback
	}
	b.incrementMessages(msgCount)
}

// failingService wraps a service to inject failures
type failingService struct {
	services.Service
	phase string
}

func (s *failingService) Initialize(ctx context.Context) error {
	return s.Service.Initialize(ctx)
}

func (s *failingService) Close(ctx context.Context) error {
	return s.Service.Close(ctx)
}

func (s *failingService) GetResource(ctx context.Context, id string) (*services.Resource, error) {
	return s.Service.GetResource(ctx, id)
}

func (s *failingService) QueryResources(ctx context.Context, query *services.ResourceQuery) ([]*services.Resource, error) {
	return s.Service.QueryResources(ctx, query)
}

func (s *failingService) Reserve(ctx context.Context, id string) (*services.Resource, error) {
	if s.phase == "reserve" {
		return nil, fmt.Errorf("simulated reserve failure")
	}
	return s.Service.Reserve(ctx, id)
}

func (s *failingService) Commit(ctx context.Context, id string) error {
	if s.phase == "commit" {
		return fmt.Errorf("simulated commit failure")
	}
	return s.Service.Commit(ctx, id)
}

func (s *failingService) Rollback(ctx context.Context, id string) error {
	if s.phase == "rollback" {
		return fmt.Errorf("simulated rollback failure")
	}
	return s.Service.Rollback(ctx, id)
}

func (s *failingService) Confirm(ctx context.Context, id string) error {
	if s.phase == "confirm" {
		return fmt.Errorf("simulated confirm failure")
	}
	return s.Service.Confirm(ctx, id)
}

func (s *failingService) CheckStatus(ctx context.Context, id string) (services.State, error) {
	if s.phase == "status" {
		return "", fmt.Errorf("simulated status check failure")
	}
	return s.Service.CheckStatus(ctx, id)
}

func (s *failingService) Healthy(ctx context.Context) bool {
	if s.phase == "health" {
		return false
	}
	return s.Service.Healthy(ctx)
}

// Helper functions for failure injection
func randomFailurePhase() string {
	phases := []string{"reserve", "commit", "rollback", "confirm", "status", "health"}
	return phases[rand.Intn(len(phases))]
}

// getPhaseLatencies extracts timing information from a transaction
func getPhaseLatencies(tx *patterns.Transaction) map[string]time.Duration {
	latencies := make(map[string]time.Duration)

	// Basic phase timing from transaction status changes
	if tx.StartTime.IsZero() || tx.EndTime.IsZero() {
		return latencies
	}

	totalDuration := tx.EndTime.Sub(tx.StartTime)
	switch tx.Status {
	case patterns.TxStatusCommitted:
		// Split total time between prepare and commit
		latencies["prepare"] = totalDuration / 2
		latencies["commit"] = totalDuration / 2
	case patterns.TxStatusConfirmed:
		// Split time between prepare, commit, and confirm
		latencies["prepare"] = totalDuration / 3
		latencies["commit"] = totalDuration / 3
		latencies["confirm"] = totalDuration / 3
	case patterns.TxStatusFailed, patterns.TxStatusRolledBack:
		latencies["rollback"] = totalDuration
	}

	return latencies
}

// Package patterns implements transaction patterns for the microbench tool
package patterns

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/ha1tch/microbench/internal/services"
)

// SagaStep represents a step in the saga
type SagaStep struct {
	Participant      *Participant
	Completed        bool
	CompensateNeeded bool
	Error            error
}

// Saga implements the Saga pattern with compensating transactions
type Saga struct {
	config *Config
	mu     sync.RWMutex
	txs    map[string]*Transaction
	steps  map[string][]*SagaStep // Maps transaction ID to its steps
}

// NewSaga creates a new Saga pattern instance
func NewSaga(config *Config) *Saga {
	return &Saga{
		config: config,
		txs:    make(map[string]*Transaction),
		steps:  make(map[string][]*SagaStep),
	}
}

// Execute runs a Saga transaction
func (p *Saga) Execute(ctx context.Context, participants []*Participant) (*Transaction, error) {
	tx := &Transaction{
		ID:           fmt.Sprintf("saga-%d", time.Now().UnixNano()),
		Status:       TxStatusInitial,
		Participants: participants,
		StartTime:    time.Now(),
	}

	// Initialize steps
	steps := make([]*SagaStep, len(participants))
	for i, participant := range participants {
		steps[i] = &SagaStep{
			Participant: participant,
		}
	}

	// Store transaction and steps
	p.mu.Lock()
	p.txs[tx.ID] = tx
	p.steps[tx.ID] = steps
	p.mu.Unlock()

	// Execute the saga
	if err := p.executeSaga(ctx, tx); err != nil {
		tx.Status = TxStatusFailed
		tx.Error = err
		tx.EndTime = time.Now()
		return tx, err
	}

	tx.Status = TxStatusCommitted
	tx.EndTime = time.Now()
	return tx, nil
}

// executeSaga processes each step of the saga
func (p *Saga) executeSaga(ctx context.Context, tx *Transaction) error {
	steps := p.steps[tx.ID]
	tx.Status = TxStatusPreparing

	// Process each step sequentially
	for i, step := range steps {
		// Create context with timeout for this step
		stepCtx, cancel := context.WithTimeout(ctx, p.config.PrepareTimeout)

		// Execute forward action with retries
		var execErr error
		for attempt := 0; p.config.RetryAttempts == -1 || attempt <= p.config.RetryAttempts; attempt++ {
			if attempt > 0 {
				select {
				case <-stepCtx.Done():
					cancel()
					return stepCtx.Err()
				case <-time.After(p.config.RetryDelay):
					// Continue with retry
				}
			}

			// First reserve the resource
			// resource, err := step.Participant.Service.Reserve(stepCtx, step.Partic    ipant.ResourceID)
			_, err := step.Participant.Service.Reserve(stepCtx, step.Participant.ResourceID)
			if err != nil {
				execErr = err
				continue
			}

			// Then immediately commit it
			err = step.Participant.Service.Commit(stepCtx, step.Participant.ResourceID)
			if err != nil {
				execErr = err
				continue
			}

			// Step succeeded
			step.Participant.State = services.StateCommitted
			step.Participant.LastUpdated = time.Now()
			step.Completed = true
			execErr = nil
			break
		}

		cancel() // Release the context

		if execErr != nil {
			// Step failed, mark for compensation
			step.Error = execErr
			step.CompensateNeeded = false // Failed before completion

			// Mark all completed steps for compensation
			for j := 0; j < i; j++ {
				steps[j].CompensateNeeded = true
			}

			// Initiate compensation
			if err := p.compensate(ctx, tx, steps, i); err != nil {
				// Log compensation error but return original error
				fmt.Printf("Compensation failed: %v\n", err)
			}

			return NewError("execution_failed", fmt.Sprintf("failed at step %d", i+1), execErr)
		}
	}

	return nil
}

// compensate performs compensation for completed steps
func (p *Saga) compensate(ctx context.Context, tx *Transaction, steps []*SagaStep, failedStep int) error {
	tx.Status = TxStatusRollingBack

	// Process compensating transactions in reverse order
	var compensationErr error
	for i := failedStep - 1; i >= 0; i-- {
		step := steps[i]
		if !step.CompensateNeeded {
			continue
		}

		// Create context with timeout for compensation
		compensateCtx, cancel := context.WithTimeout(ctx, p.config.RollbackTimeout)

		// Execute compensating action with retries
		var lastErr error
		for attempt := 0; p.config.RetryAttempts == -1 || attempt <= p.config.RetryAttempts; attempt++ {
			if attempt > 0 {
				select {
				case <-compensateCtx.Done():
					cancel()
					return compensateCtx.Err()
				case <-time.After(p.config.RetryDelay):
					// Continue with retry
				}
			}

			err := step.Participant.Service.Rollback(compensateCtx, step.Participant.ResourceID)
			if err == nil {
				step.Participant.State = services.StateRollback
				step.Participant.LastUpdated = time.Now()
				step.CompensateNeeded = false
				break
			}
			lastErr = err
		}

		cancel() // Release the context

		if lastErr != nil {
			compensationErr = lastErr
			// Continue with other compensations even if one fails
		}
	}

	if compensationErr != nil {
		return NewError("compensation_failed", "failed to compensate all steps", compensationErr)
	}

	tx.Status = TxStatusRolledBack
	return nil
}

// Rollback initiates compensation for a saga
func (p *Saga) Rollback(ctx context.Context, tx *Transaction) error {
	if tx.Status == TxStatusRolledBack {
		return nil // Already rolled back
	}

	steps, exists := p.steps[tx.ID]
	if !exists {
		return NewError("not_found", "transaction steps not found", nil)
	}

	// Mark all completed steps for compensation
	for _, step := range steps {
		if step.Completed {
			step.CompensateNeeded = true
		}
	}

	// Perform compensation
	if err := p.compensate(ctx, tx, steps, len(steps)); err != nil {
		tx.Status = TxStatusFailed
		tx.Error = err
		return err
	}

	tx.Status = TxStatusRolledBack
	tx.EndTime = time.Now()
	return nil
}

// GetStatus returns the current transaction status
func (p *Saga) GetStatus(ctx context.Context, txID string) (*Transaction, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	tx, exists := p.txs[txID]
	if !exists {
		return nil, NewError("not_found", fmt.Sprintf("transaction not found: %s", txID), nil)
	}

	return tx, nil
}

// Helper method to build saga execution order
func (p *Saga) buildExecutionOrder(participants []*Participant) []*Participant {
	// In a real implementation, this would:
	// 1. Analyze dependencies between participants
	// 2. Create an optimal execution order
	// 3. Consider parallel execution possibilities

	// For this implementation, we'll use the order as provided
	return participants
}

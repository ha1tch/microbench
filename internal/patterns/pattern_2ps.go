// Package patterns implements transaction patterns for the microbench tool
package patterns

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/ha1tch/microbench/internal/services"
)

// TwoPhaseSaga implements the Two-Phase Saga (2PS) pattern
type TwoPhaseSaga struct {
	config *Config
	mu     sync.RWMutex
	txs    map[string]*Transaction
}

// NewTwoPhaseSaga creates a new 2PS pattern instance
func NewTwoPhaseSaga(config *Config) *TwoPhaseSaga {
	return &TwoPhaseSaga{
		config: config,
		txs:    make(map[string]*Transaction),
	}
}

// Execute runs a 2PS transaction
func (p *TwoPhaseSaga) Execute(ctx context.Context, participants []*Participant) (*Transaction, error) {
	tx := &Transaction{
		ID:           fmt.Sprintf("2ps-%d", time.Now().UnixNano()),
		Status:       TxStatusInitial,
		Participants: participants,
		StartTime:    time.Now(),
	}

	// Store transaction
	p.mu.Lock()
	p.txs[tx.ID] = tx
	p.mu.Unlock()

	// Phase 1: Resource Reservation
	if err := p.reserve(ctx, tx); err != nil {
		tx.Status = TxStatusFailed
		tx.Error = err
		tx.EndTime = time.Now()
		return tx, err
	}

	// Phase 2: Commit
	if err := p.commit(ctx, tx); err != nil {
		tx.Status = TxStatusFailed
		tx.Error = err
		tx.EndTime = time.Now()
		return tx, err
	}

	tx.Status = TxStatusCommitted
	tx.EndTime = time.Now()
	return tx, nil
}

// reserve implements the reservation phase of 2PS
func (p *TwoPhaseSaga) reserve(ctx context.Context, tx *Transaction) error {
	tx.Status = TxStatusPreparing

	// Create context with timeout
	reserveCtx, cancel := context.WithTimeout(ctx, p.config.PrepareTimeout)
	defer cancel()

	// Reserve resources in parallel
	var wg sync.WaitGroup
	errChan := make(chan error, len(tx.Participants))

	for _, participant := range tx.Participants {
		wg.Add(1)
		go func(part *Participant) {
			defer wg.Done()

			// Attempt reservation with retries
			var lastErr error
			for attempt := 0; p.config.RetryAttempts == -1 || attempt <= p.config.RetryAttempts; attempt++ {
				if attempt > 0 {
					select {
					case <-reserveCtx.Done():
						errChan <- reserveCtx.Err()
						return
					case <-time.After(p.config.RetryDelay):
						// Continue with retry
					}
				}

				resource, err := part.Service.Reserve(reserveCtx, part.ResourceID)
				if err == nil {
					part.State = resource.State
					part.LastUpdated = time.Now()
					return
				}
				lastErr = err

				if isTerminalError(lastErr) {
					errChan <- lastErr
					return
				}
			}
			errChan <- lastErr
		}(participant)
	}

	// Wait for all reservations
	go func() {
		wg.Wait()
		close(errChan)
	}()

	// Check for errors
	var reservationErr error
	for err := range errChan {
		if err != nil {
			reservationErr = err
			break
		}
	}

	if reservationErr != nil {
		// Reservation failed, initiate cleanup
		rollbackErr := p.cleanup(ctx, tx)
		if rollbackErr != nil {
			fmt.Printf("Cleanup failed during reservation phase: %v\n", rollbackErr)
		}
		return NewError("reservation_failed", "failed to reserve resources", reservationErr)
	}

	tx.Status = TxStatusPrepared
	return nil
}

// commit implements the commit phase of 2PS
func (p *TwoPhaseSaga) commit(ctx context.Context, tx *Transaction) error {
	if tx.Status != TxStatusPrepared {
		return ErrInvalidStatus
	}

	tx.Status = TxStatusCommitting

	// Create context with timeout
	commitCtx, cancel := context.WithTimeout(ctx, p.config.CommitTimeout)
	defer cancel()

	// Commit each resource in parallel
	var wg sync.WaitGroup
	errChan := make(chan error, len(tx.Participants))

	for _, participant := range tx.Participants {
		wg.Add(1)
		go func(part *Participant) {
			defer wg.Done()

			// Attempt commit with retries
			var lastErr error
			for attempt := 0; p.config.RetryAttempts == -1 || attempt <= p.config.RetryAttempts; attempt++ {
				if attempt > 0 {
					select {
					case <-commitCtx.Done():
						errChan <- commitCtx.Err()
						return
					case <-time.After(p.config.RetryDelay):
						// Continue with retry
					}
				}

				if err := part.Service.Commit(commitCtx, part.ResourceID); err == nil {
					part.State = services.StateCommitted
					part.LastUpdated = time.Now()
					return
				} else {
					lastErr = err
					if isTerminalError(lastErr) {
						errChan <- lastErr
						return
					}
				}
			}
			errChan <- lastErr
		}(participant)
	}

	// Wait for all commits
	go func() {
		wg.Wait()
		close(errChan)
	}()

	// Check for errors
	var commitErr error
	for err := range errChan {
		if err != nil {
			commitErr = err
			break
		}
	}

	if commitErr != nil {
		// Commit failed, attempt cleanup
		rollbackErr := p.cleanup(ctx, tx)
		if rollbackErr != nil {
			fmt.Printf("Cleanup failed during commit phase: %v\n", rollbackErr)
		}
		return NewError("commit_failed", "failed to commit resources", commitErr)
	}

	return nil
}

// cleanup releases resources in case of failure
func (p *TwoPhaseSaga) cleanup(ctx context.Context, tx *Transaction) error {
	tx.Status = TxStatusRollingBack

	// Create context with timeout
	cleanupCtx, cancel := context.WithTimeout(ctx, p.config.RollbackTimeout)
	defer cancel()

	// Release each resource in parallel
	var wg sync.WaitGroup
	errChan := make(chan error, len(tx.Participants))

	for _, participant := range tx.Participants {
		// Skip if not reserved
		if participant.State != services.StateReserved && 
		   participant.State != services.StateCommitted {
			continue
		}

		wg.Add(1)
		go func(part *Participant) {
			defer wg.Done()

			// Attempt rollback with retries
			var lastErr error
			for attempt := 0; p.config.RetryAttempts == -1 || attempt <= p.config.RetryAttempts; attempt++ {
				if attempt > 0 {
					select {
					case <-cleanupCtx.Done():
						errChan <- cleanupCtx.Err()
						return
					case <-time.After(p.config.RetryDelay):
						// Continue with retry
					}
				}

				if err := part.Service.Rollback(cleanupCtx, part.ResourceID); err == nil {
					part.State = services.StateRollback
					part.LastUpdated = time.Now()
					return
				} else {
					lastErr = err
				}
			}
			errChan <- lastErr
		}(participant)
	}

	// Wait for all cleanups
	go func() {
		wg.Wait()
		close(errChan)
	}()

	// Check for errors
	var cleanupErr error
	for err := range errChan {
		if err != nil {
			cleanupErr = err
			// Continue cleanup even if some fail
		}
	}

	if cleanupErr != nil {
		tx.Status = TxStatusFailed
		return NewError("cleanup_failed", "failed to cleanup resources", cleanupErr)
	}

	tx.Status = TxStatusRolledBack
	return nil
}

// Rollback attempts to roll back a 2PS transaction
func (p *TwoPhaseSaga) Rollback(ctx context.Context, tx *Transaction) error {
	if tx.Status == TxStatusRolledBack {
		return nil // Already rolled back
	}

	// Cannot rollback committed transactions
	if tx.Status == TxStatusCommitted {
		return NewError("rollback_denied", "cannot rollback committed transaction", nil)
	}

	return p.cleanup(ctx, tx)
}

// GetStatus returns the current transaction status
func (p *TwoPhaseSaga) GetStatus(ctx context.Context, txID string) (*Transaction, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	tx, exists := p.txs[txID]
	if !exists {
		return nil, NewError("not_found", fmt.Sprintf("transaction not found: %s", txID), nil)
	}

	return tx, nil
}

// isTerminalError determines if an error should stop retries
func isTerminalError(err error) bool {
	if err == nil {
		return false
	}

	// Check for specific error types that indicate retries would be futile
	switch err {
	case services.ErrNotFound,
		 services.ErrAlreadyExists,
		 services.ErrInvalidState:
		return true
	default:
		return false
	}
}

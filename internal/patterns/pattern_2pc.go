// Package patterns implements transaction patterns for the microbench tool
package patterns

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/ha1tch/microbench/internal/services"
)

// TwoPhaseCommit implements the classic 2PC pattern
type TwoPhaseCommit struct {
	config *Config
	mu     sync.RWMutex
	txs    map[string]*Transaction
}

// NewTwoPhaseCommit creates a new 2PC pattern instance
func NewTwoPhaseCommit(config *Config) *TwoPhaseCommit {
	return &TwoPhaseCommit{
		config: config,
		txs:    make(map[string]*Transaction),
	}
}

// Execute runs a 2PC transaction
func (p *TwoPhaseCommit) Execute(ctx context.Context, participants []*Participant) (*Transaction, error) {
	tx := &Transaction{
		ID:           fmt.Sprintf("2pc-%d", time.Now().UnixNano()),
		Status:       TxStatusInitial,
		Participants: participants,
		StartTime:    time.Now(),
	}

	// Store transaction
	p.mu.Lock()
	p.txs[tx.ID] = tx
	p.mu.Unlock()

	// Phase 1: Prepare
	if err := p.prepare(ctx, tx); err != nil {
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

// prepare implements the prepare phase of 2PC
func (p *TwoPhaseCommit) prepare(ctx context.Context, tx *Transaction) error {
	tx.Status = TxStatusPreparing

	// Create context with timeout
	prepareCtx, cancel := context.WithTimeout(ctx, p.config.PrepareTimeout)
	defer cancel()

	// Prepare each participant
	var wg sync.WaitGroup
	errChan := make(chan error, len(tx.Participants))

	for _, participant := range tx.Participants {
		wg.Add(1)
		go func(part *Participant) {
			defer wg.Done()

			// Attempt prepare with retries
			var lastErr error
			for attempt := 0; p.config.RetryAttempts == -1 || attempt <= p.config.RetryAttempts; attempt++ {
				if attempt > 0 {
					select {
					case <-prepareCtx.Done():
						errChan <- prepareCtx.Err()
						return
					case <-time.After(p.config.RetryDelay):
						// Continue with retry
					}
				}

				resource, err := part.Service.Reserve(prepareCtx, part.ResourceID)
				if err == nil {
					part.State = resource.State
					part.LastUpdated = time.Now()
					return
				}
				lastErr = err
			}
			errChan <- lastErr
		}(participant)
	}

	// Wait for all prepares to complete
	go func() {
		wg.Wait()
		close(errChan)
	}()

	// Check for errors
	for err := range errChan {
		if err != nil {
			// Prepare failed, initiate rollback
			rollbackErr := p.Rollback(ctx, tx)
			if rollbackErr != nil {
				// Log rollback error but return original error
				fmt.Printf("Rollback failed during prepare phase: %v\n", rollbackErr)
			}
			return NewError("prepare_failed", "failed to prepare all participants", err)
		}
	}

	tx.Status = TxStatusPrepared
	return nil
}

// commit implements the commit phase of 2PC
func (p *TwoPhaseCommit) commit(ctx context.Context, tx *Transaction) error {
	if tx.Status != TxStatusPrepared {
		return ErrInvalidStatus
	}

	tx.Status = TxStatusCommitting

	// Create context with timeout
	commitCtx, cancel := context.WithTimeout(ctx, p.config.CommitTimeout)
	defer cancel()

	// Commit each participant
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
				}
			}
			errChan <- lastErr
		}(participant)
	}

	// Wait for all commits to complete
	go func() {
		wg.Wait()
		close(errChan)
	}()

	// Check for errors
	for err := range errChan {
		if err != nil {
			return NewError("commit_failed", "failed to commit all participants", err)
		}
	}

	return nil
}

// Rollback attempts to undo a 2PC transaction
func (p *TwoPhaseCommit) Rollback(ctx context.Context, tx *Transaction) error {
	if tx.Status == TxStatusRolledBack {
		return nil // Already rolled back
	}

	tx.Status = TxStatusRollingBack

	// Create context with timeout
	rollbackCtx, cancel := context.WithTimeout(ctx, p.config.RollbackTimeout)
	defer cancel()

	// Rollback each participant
	var wg sync.WaitGroup
	errChan := make(chan error, len(tx.Participants))

	for _, participant := range tx.Participants {
		// Skip participants that haven't been prepared
		if participant.State == services.StateInitial {
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
					case <-rollbackCtx.Done():
						errChan <- rollbackCtx.Err()
						return
					case <-time.After(p.config.RetryDelay):
						// Continue with retry
					}
				}

				if err := part.Service.Rollback(rollbackCtx, part.ResourceID); err == nil {
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

	// Wait for all rollbacks to complete
	go func() {
		wg.Wait()
		close(errChan)
	}()

	// Check for errors
	var rollbackErr error
	for err := range errChan {
		if err != nil {
			rollbackErr = err
		}
	}

	if rollbackErr != nil {
		tx.Status = TxStatusFailed
		tx.Error = NewError("rollback_failed", "failed to rollback all participants", rollbackErr)
		return tx.Error
	}

	tx.Status = TxStatusRolledBack
	tx.EndTime = time.Now()
	return nil
}

// GetStatus returns the current transaction status
func (p *TwoPhaseCommit) GetStatus(ctx context.Context, txID string) (*Transaction, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	tx, exists := p.txs[txID]
	if !exists {
		return nil, NewError("not_found", fmt.Sprintf("transaction not found: %s", txID), nil)
	}

	return tx, nil
}

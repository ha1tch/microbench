//------------------------------------------------------------------------------
// PART 2: Status verification, commit, cleanup, and management functions
//------------------------------------------------------------------------------

package patterns

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/ha1tch/microbench/internal/services"
)

// verifyStatus implements the status check phase of 2.5PS
func (p *TwoAndHalfPhaseSaga) verifyStatus(ctx context.Context, tx *Transaction) error {
	if tx.Status != TxStatusPrepared {
		return ErrInvalidStatus
	}

	// Create context with timeout
	verifyCtx, cancel := context.WithTimeout(ctx, p.config.PrepareTimeout)
	defer cancel()

	// Check status of all participants in parallel
	var wg sync.WaitGroup
	resultChan := make(chan statusCheckResult, len(tx.Participants))

	for _, participant := range tx.Participants {
		wg.Add(1)
		go func(part *Participant) {
			defer wg.Done()

			var result statusCheckResult
			result.participant = part
			result.timestamp = time.Now()

			// Attempt status check with retries
			for attempt := 0; attempt < p.StatusCheckConfig.MaxCheckAttempts; attempt++ {
				if attempt > 0 {
					select {
					case <-verifyCtx.Done():
						result.error = verifyCtx.Err()
						resultChan <- result
						return
					case <-time.After(p.config.RetryDelay):
						// Continue with retry
					}
				}

				state, err := part.Service.CheckStatus(verifyCtx, part.ResourceID)
				if err == nil {
					result.state = state
					result.error = nil
					resultChan <- result
					return
				}
				result.error = err

				if isTerminalError(err) {
					resultChan <- result
					return
				}
			}
			resultChan <- result
		}(participant)
	}

	// Wait for all status checks
	go func() {
		wg.Wait()
		close(resultChan)
	}()

	// Process results
	successCount := 0
	var verificationErrors []error
	requiredCount := len(tx.Participants)
	if p.StatusCheckConfig.ParticipantThreshold > 0 {
		requiredCount = p.StatusCheckConfig.ParticipantThreshold
	}

	for result := range resultChan {
		if result.error != nil {
			verificationErrors = append(verificationErrors,
				NewError("status_check_failed",
					fmt.Sprintf("failed to verify participant %s", result.participant.ResourceID),
					result.error))
			continue
		}

		// Check if state is acceptable
		stateValid := false
		for _, validState := range p.StatusCheckConfig.RequiredStates {
			if result.state == validState {
				stateValid = true
				break
			}
		}

		if !stateValid {
			verificationErrors = append(verificationErrors,
				NewError("invalid_state",
					fmt.Sprintf("participant %s in unexpected state: %s",
						result.participant.ResourceID, result.state),
					nil))
			continue
		}

		successCount++
	}

	// Check if we met the threshold
	if successCount < requiredCount {
		// Not enough participants verified successfully
		rollbackErr := p.cleanup(ctx, tx)
		if rollbackErr != nil {
			fmt.Printf("Cleanup failed during status verification: %v\n", rollbackErr)
		}

		// Combine all verification errors into one
		return NewError("verification_failed",
			fmt.Sprintf("only %d/%d participants verified successfully", successCount, requiredCount),
			fmt.Errorf("%v", verificationErrors))
	}

	return nil
}

// commit implements the commit phase of 2.5PS
func (p *TwoAndHalfPhaseSaga) commit(ctx context.Context, tx *Transaction) error {
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
func (p *TwoAndHalfPhaseSaga) cleanup(ctx context.Context, tx *Transaction) error {
	tx.Status = TxStatusRollingBack

	// Create context with timeout
	cleanupCtx, cancel := context.WithTimeout(ctx, p.config.RollbackTimeout)
	defer cancel()

	// Release each resource in parallel
	var wg sync.WaitGroup
	errChan := make(chan error, len(tx.Participants))

	for _, participant := range tx.Participants {
		// Skip if not reserved or committed
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
		}
	}

	if cleanupErr != nil {
		tx.Status = TxStatusFailed
		return NewError("cleanup_failed", "failed to cleanup resources", cleanupErr)
	}

	tx.Status = TxStatusRolledBack
	return nil
}

// Rollback attempts to roll back a 2.5PS transaction
func (p *TwoAndHalfPhaseSaga) Rollback(ctx context.Context, tx *Transaction) error {
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
func (p *TwoAndHalfPhaseSaga) GetStatus(ctx context.Context, txID string) (*Transaction, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	tx, exists := p.txs[txID]
	if !exists {
		return nil, NewError("not_found", fmt.Sprintf("transaction not found: %s", txID), nil)
	}

	return tx, nil
}

// SetStatusCheckConfig allows runtime configuration of status checks
func (p *TwoAndHalfPhaseSaga) SetStatusCheckConfig(config struct {
	Enabled              bool
	ParticipantThreshold int
	MaxCheckAttempts     int
	RequiredStates       []services.State
}) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.StatusCheckConfig = config
}

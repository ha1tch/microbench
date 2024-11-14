//------------------------------------------------------------------------------
// PART 1: Base structure and reservation phase
//------------------------------------------------------------------------------

// Package patterns implements transaction patterns for the microbench tool
package patterns

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/ha1tch/microbench/internal/services"
)

// TwoAndHalfPhaseSaga implements the Two-and-Half Phase Saga (2.5PS) pattern
type TwoAndHalfPhaseSaga struct {
	config *Config
	mu     sync.RWMutex
	txs    map[string]*Transaction

	// StatusCheckConfig allows customization of status checks
	StatusCheckConfig struct {
		// Enabled indicates if status checks are required
		Enabled bool

		// ParticipantThreshold is the minimum number of participants
		// that need successful status checks (0 = all)
		ParticipantThreshold int

		// MaxCheckAttempts is the maximum number of status check attempts
		MaxCheckAttempts int

		// RequiredStates are the states that must be verified
		RequiredStates []services.State
	}
}

// Status check results
type statusCheckResult struct {
	participant *Participant
	state      services.State
	error      error
	timestamp  time.Time
}

// NewTwoAndHalfPhaseSaga creates a new 2.5PS pattern instance
func NewTwoAndHalfPhaseSaga(config *Config) *TwoAndHalfPhaseSaga {
	return &TwoAndHalfPhaseSaga{
		config: config,
		txs:    make(map[string]*Transaction),
		StatusCheckConfig: struct {
			Enabled              bool
			ParticipantThreshold int
			MaxCheckAttempts     int
			RequiredStates       []services.State
		}{
			Enabled:              true,
			ParticipantThreshold: 0, // All participants by default
			MaxCheckAttempts:     3,
			RequiredStates:       []services.State{services.StateReserved},
		},
	}
}

// Execute runs a 2.5PS transaction
func (p *TwoAndHalfPhaseSaga) Execute(ctx context.Context, participants []*Participant) (*Transaction, error) {
	tx := &Transaction{
		ID:           fmt.Sprintf("25ps-%d", time.Now().UnixNano()),
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

	// Optional Status Check Phase (the "Half" phase)
	if p.StatusCheckConfig.Enabled {
		if err := p.verifyStatus(ctx, tx); err != nil {
			tx.Status = TxStatusFailed
			tx.Error = err
			tx.EndTime = time.Now()
			return tx, err
		}
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

// reserve implements the reservation phase of 2.5PS
func (p *TwoAndHalfPhaseSaga) reserve(ctx context.Context, tx *Transaction) error {
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

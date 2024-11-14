// Package patterns implements transaction patterns for the microbench tool
package patterns

import (
	"context"
	"time"

	"github.com/ha1tch/microbench/internal/services"
)

// TransactionStatus represents the current status of a transaction
type TransactionStatus string

const (
	// TxStatusInitial represents a new transaction
	TxStatusInitial TransactionStatus = "initial"

	// TxStatusPreparing indicates preparation phase is in progress
	TxStatusPreparing TransactionStatus = "preparing"

	// TxStatusPrepared indicates all resources are prepared
	TxStatusPrepared TransactionStatus = "prepared"

	// TxStatusCommitting indicates commit phase is in progress
	TxStatusCommitting TransactionStatus = "committing"

	// TxStatusCommitted indicates transaction is committed
	TxStatusCommitted TransactionStatus = "committed"

	// TxStatusConfirming indicates confirmation phase is in progress (3PS)
	TxStatusConfirming TransactionStatus = "confirming"

	// TxStatusConfirmed indicates transaction is confirmed (3PS)
	TxStatusConfirmed TransactionStatus = "confirmed"

	// TxStatusRollingBack indicates rollback is in progress
	TxStatusRollingBack TransactionStatus = "rolling_back"

	// TxStatusRolledBack indicates transaction was rolled back
	TxStatusRolledBack TransactionStatus = "rolled_back"

	// TxStatusFailed indicates transaction failed
	TxStatusFailed TransactionStatus = "failed"
)

// Participant represents a service participating in a transaction
type Participant struct {
	// Service is the participating service
	Service services.Service

	// ResourceID identifies the resource being managed
	ResourceID string

	// State tracks the participant's state
	State services.State

	// LastUpdated timestamp of last state change
	LastUpdated time.Time

	// Error if any occurred during operations
	Error error
}

// Transaction represents a distributed transaction
type Transaction struct {
	// ID uniquely identifies the transaction
	ID string

	// Status indicates current transaction status
	Status TransactionStatus

	// Participants in the transaction
	Participants []*Participant

	// StartTime when transaction began
	StartTime time.Time

	// EndTime when transaction completed/failed
	EndTime time.Time

	// Error if transaction failed
	Error error
}

// Pattern defines the interface for transaction patterns
type Pattern interface {
	// Execute runs a transaction with the given participants
	Execute(ctx context.Context, participants []*Participant) (*Transaction, error)

	// Rollback attempts to undo a transaction
	Rollback(ctx context.Context, tx *Transaction) error

	// GetStatus returns the current transaction status
	GetStatus(ctx context.Context, txID string) (*Transaction, error)
}

// Common errors
var (
	ErrPrepareFailed = PatternError{Code: "prepare_failed", Message: "failed to prepare participants"}
	ErrCommitFailed  = PatternError{Code: "commit_failed", Message: "failed to commit transaction"}
	ErrRollbackFailed = PatternError{Code: "rollback_failed", Message: "failed to rollback transaction"}
	ErrInvalidStatus = PatternError{Code: "invalid_status", Message: "invalid transaction status"}
)

// PatternError represents a pattern-specific error
type PatternError struct {
	Code    string // Machine-readable error code
	Message string // Human-readable error message
	Cause   error  // Underlying error if any
}

func (e PatternError) Error() string {
	if e.Cause != nil {
		return e.Message + ": " + e.Cause.Error()
	}
	return e.Message
}

// NewError creates a new pattern error
func NewError(code string, message string, cause error) PatternError {
	return PatternError{
		Code:    code,
		Message: message,
		Cause:   cause,
	}
}

// Config holds pattern configuration
type Config struct {
	// Timeouts for different phases
	PrepareTimeout  time.Duration
	CommitTimeout   time.Duration
	RollbackTimeout time.Duration
	ConfirmTimeout  time.Duration

	// RetryAttempts for each phase (-1 for infinite)
	RetryAttempts int

	// RetryDelay between attempts
	RetryDelay time.Duration
}

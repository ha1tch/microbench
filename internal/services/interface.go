// Package services provides core service interfaces for the microbench tool
package services

import (
	"context"
	"time"
)

// State represents a service resource state
type State string

// Common service states
const (
	StateInitial     State = "initial"
	StatePending     State = "pending"
	StateReserved    State = "reserved"
	StateCommitted   State = "committed"
	StateRollback    State = "rollback"
	StateConfirmed   State = "confirmed"
	StateError       State = "error"
)

// Resource represents a service resource that can be managed in a transaction
type Resource struct {
	// ID uniquely identifies the resource
	ID string

	// Type identifies the kind of resource
	Type string

	// State represents the current state of the resource
	State State

	// Data contains resource-specific data
	Data map[string]interface{}

	// Version for optimistic locking
	Version int64

	// LastModified timestamp
	LastModified time.Time
}

// ResourceQuery defines criteria for finding resources
type ResourceQuery struct {
	// IDs to find specific resources
	IDs []string

	// Types to filter by resource type
	Types []string

	// States to filter by resource state
	States []State

	// Limit maximum number of results
	Limit int

	// Offset for pagination
	Offset int
}

// Service defines the interface that must be implemented by all services
type Service interface {
	// Initialize prepares the service for operation
	Initialize(ctx context.Context) error

	// Close cleanly shuts down the service
	Close(ctx context.Context) error

	// GetResource retrieves a specific resource
	GetResource(ctx context.Context, id string) (*Resource, error)

	// QueryResources finds resources matching criteria
	QueryResources(ctx context.Context, query *ResourceQuery) ([]*Resource, error)

	// Reserve attempts to reserve a resource for a transaction
	Reserve(ctx context.Context, id string) (*Resource, error)

	// Commit finalizes a resource reservation
	Commit(ctx context.Context, id string) error

	// Rollback cancels a resource reservation
	Rollback(ctx context.Context, id string) error

	// Confirm acknowledges a committed resource (for 3PS)
	Confirm(ctx context.Context, id string) error

	// CheckStatus verifies resource state (for 2.5PS)
	CheckStatus(ctx context.Context, id string) (State, error)

	// Healthy returns true if the service is operating normally
	Healthy(ctx context.Context) bool
}

// ServiceType identifies the type of service
type ServiceType string

const (
	// ServiceOrder identifies the order management service
	ServiceOrder ServiceType = "order"

	// ServiceInventory identifies the inventory management service
	ServiceInventory ServiceType = "inventory"

	// ServicePayment identifies the payment processing service
	ServicePayment ServiceType = "payment"
)

// Common errors
var (
	ErrNotFound           = ServiceError{Code: "not_found", Message: "resource not found"}
	ErrAlreadyExists      = ServiceError{Code: "already_exists", Message: "resource already exists"}
	ErrInvalidState       = ServiceError{Code: "invalid_state", Message: "invalid resource state"}
	ErrConcurrencyConflict = ServiceError{Code: "concurrency_conflict", Message: "resource version conflict"}
	ErrNotInitialized     = ServiceError{Code: "not_initialized", Message: "service not initialized"}
	ErrAlreadyClosed      = ServiceError{Code: "already_closed", Message: "service already closed"}
)

// ServiceError represents a service-specific error
type ServiceError struct {
	Code    string // Machine-readable error code
	Message string // Human-readable error message
	Cause   error  // Underlying error if any
}

func (e ServiceError) Error() string {
	if e.Cause != nil {
		return e.Message + ": " + e.Cause.Error()
	}
	return e.Message
}

// NewError creates a new service error
func NewError(code string, message string, cause error) ServiceError {
	return ServiceError{
		Code:    code,
		Message: message,
		Cause:   cause,
	}
}

// Config holds service configuration
type Config struct {
	// ServiceType identifies the type of service
	ServiceType ServiceType

	// StoragePath for persistent storage if needed
	StoragePath string

	// BrokerConfig for message broker connection
	BrokerConfig interface{}

	// RetryAttempts for operations (-1 for infinite)
	RetryAttempts int

	// RetryDelay between retry attempts
	RetryDelay time.Duration
}

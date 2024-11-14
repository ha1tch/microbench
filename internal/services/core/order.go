// Package core implements the core services for the microbench tool
package core

import (
	"context"
	//	"fmt"
	"sync"
	"time"

	"github.com/ha1tch/microbench/internal/services"
)

// Order represents an order in the system
type Order struct {
	*services.Resource
	CustomerID string
	Items      []OrderItem
	Total      float64
}

// OrderItem represents an item in an order
type OrderItem struct {
	ProductID string
	Quantity  int
	Price     float64
}

// OrderService implements the Service interface for order management
type OrderService struct {
	mu        sync.RWMutex
	config    *services.Config
	orders    map[string]*Order
	initiated bool
	closed    bool
}

// NewOrderService creates a new order service instance
func NewOrderService(config *services.Config) (*OrderService, error) {
	if config.ServiceType != services.ServiceOrder {
		return nil, services.NewError("invalid_service_type", "config must be for order service", nil)
	}

	return &OrderService{
		config: config,
		orders: make(map[string]*Order),
	}, nil
}

// Initialize prepares the service for operation
func (s *OrderService) Initialize(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.initiated {
		return nil
	}

	if s.closed {
		return services.ErrAlreadyClosed
	}

	// TODO: Initialize storage if needed
	// TODO: Connect to message broker if needed

	s.initiated = true
	return nil
}

// Close cleanly shuts down the service
func (s *OrderService) Close(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.initiated {
		return services.ErrNotInitialized
	}

	if s.closed {
		return services.ErrAlreadyClosed
	}

	// TODO: Close storage if needed
	// TODO: Disconnect from message broker if needed

	s.closed = true
	return nil
}

// GetResource retrieves a specific order
func (s *OrderService) GetResource(ctx context.Context, id string) (*services.Resource, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if !s.initiated {
		return nil, services.ErrNotInitialized
	}

	if s.closed {
		return nil, services.ErrAlreadyClosed
	}

	order, exists := s.orders[id]
	if !exists {
		return nil, services.ErrNotFound
	}

	return order.Resource, nil
}

// QueryResources finds orders matching criteria
func (s *OrderService) QueryResources(ctx context.Context, query *services.ResourceQuery) ([]*services.Resource, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if !s.initiated {
		return nil, services.ErrNotInitialized
	}

	if s.closed {
		return nil, services.ErrAlreadyClosed
	}

	var results []*services.Resource

	// Apply filters from query
	for _, order := range s.orders {
		if s.matchesQuery(order.Resource, query) {
			results = append(results, order.Resource)
		}
	}

	// Apply pagination
	if query.Offset >= len(results) {
		return []*services.Resource{}, nil
	}

	end := query.Offset + query.Limit
	if end > len(results) || query.Limit == 0 {
		end = len(results)
	}

	return results[query.Offset:end], nil
}

// Reserve attempts to reserve an order for a transaction
func (s *OrderService) Reserve(ctx context.Context, id string) (*services.Resource, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.initiated {
		return nil, services.ErrNotInitialized
	}

	if s.closed {
		return nil, services.ErrAlreadyClosed
	}

	order, exists := s.orders[id]
	if !exists {
		return nil, services.ErrNotFound
	}

	// Check if order can be reserved
	if order.Resource.State != services.StateInitial &&
		order.Resource.State != services.StateRollback {
		return nil, services.ErrInvalidState
	}

	// Update order state
	order.Resource.State = services.StateReserved
	order.Resource.Version++
	order.Resource.LastModified = time.Now()

	return order.Resource, nil
}

// Commit finalizes an order reservation
func (s *OrderService) Commit(ctx context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.initiated {
		return services.ErrNotInitialized
	}

	if s.closed {
		return services.ErrAlreadyClosed
	}

	order, exists := s.orders[id]
	if !exists {
		return services.ErrNotFound
	}

	// Check if order can be committed
	if order.Resource.State != services.StateReserved {
		return services.ErrInvalidState
	}

	// Update order state
	order.Resource.State = services.StateCommitted
	order.Resource.Version++
	order.Resource.LastModified = time.Now()

	return nil
}

// Rollback cancels an order reservation
func (s *OrderService) Rollback(ctx context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.initiated {
		return services.ErrNotInitialized
	}

	if s.closed {
		return services.ErrAlreadyClosed
	}

	order, exists := s.orders[id]
	if !exists {
		return services.ErrNotFound
	}

	// Any state can be rolled back except Confirmed
	if order.Resource.State == services.StateConfirmed {
		return services.ErrInvalidState
	}

	// Update order state
	order.Resource.State = services.StateRollback
	order.Resource.Version++
	order.Resource.LastModified = time.Now()

	return nil
}

// Confirm acknowledges a committed order (for 3PS)
func (s *OrderService) Confirm(ctx context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.initiated {
		return services.ErrNotInitialized
	}

	if s.closed {
		return services.ErrAlreadyClosed
	}

	order, exists := s.orders[id]
	if !exists {
		return services.ErrNotFound
	}

	// Check if order can be confirmed
	if order.Resource.State != services.StateCommitted {
		return services.ErrInvalidState
	}

	// Update order state
	order.Resource.State = services.StateConfirmed
	order.Resource.Version++
	order.Resource.LastModified = time.Now()

	return nil
}

// CheckStatus verifies order state (for 2.5PS)
func (s *OrderService) CheckStatus(ctx context.Context, id string) (services.State, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if !s.initiated {
		return "", services.ErrNotInitialized
	}

	if s.closed {
		return "", services.ErrAlreadyClosed
	}

	order, exists := s.orders[id]
	if !exists {
		return "", services.ErrNotFound
	}

	return order.Resource.State, nil
}

// Healthy returns true if the service is operating normally
func (s *OrderService) Healthy(ctx context.Context) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.initiated && !s.closed
}

// Helper function to check if a resource matches query criteria
func (s *OrderService) matchesQuery(resource *services.Resource, query *services.ResourceQuery) bool {
	// Check IDs
	if len(query.IDs) > 0 {
		found := false
		for _, id := range query.IDs {
			if resource.ID == id {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}

	// Check Types
	if len(query.Types) > 0 {
		found := false
		for _, t := range query.Types {
			if resource.Type == t {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}

	// Check States
	if len(query.States) > 0 {
		found := false
		for _, s := range query.States {
			if resource.State == s {
				found = true
				break
			}
		}
		if !found {
			return false
		}
	}

	return true
}

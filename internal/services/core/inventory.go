// Package core implements the core services for the microbench tool
package core

import (
	"context"
	"sync"
	"time"

	"github.com/ha1tch/microbench/internal/services"
)

// InventoryItem represents an item in inventory
type InventoryItem struct {
	*services.Resource
	ProductID   string
	Quantity    int
	Reserved    int
	UnitPrice   float64
	ReorderPoint int
}

// InventoryService implements the Service interface for inventory management
type InventoryService struct {
	mu        sync.RWMutex
	config    *services.Config
	inventory map[string]*InventoryItem
	initiated bool
	closed    bool
}

// NewInventoryService creates a new inventory service instance
func NewInventoryService(config *services.Config) (*InventoryService, error) {
	if config.ServiceType != services.ServiceInventory {
		return nil, services.NewError("invalid_service_type", "config must be for inventory service", nil)
	}

	return &InventoryService{
		config:    config,
		inventory: make(map[string]*InventoryItem),
	}, nil
}

// Initialize prepares the service for operation
func (s *InventoryService) Initialize(ctx context.Context) error {
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
func (s *InventoryService) Close(ctx context.Context) error {
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

// GetResource retrieves a specific inventory item
func (s *InventoryService) GetResource(ctx context.Context, id string) (*services.Resource, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if !s.initiated {
		return nil, services.ErrNotInitialized
	}

	if s.closed {
		return nil, services.ErrAlreadyClosed
	}

	item, exists := s.inventory[id]
	if !exists {
		return nil, services.ErrNotFound
	}

	return item.Resource, nil
}

// QueryResources finds inventory items matching criteria
func (s *InventoryService) QueryResources(ctx context.Context, query *services.ResourceQuery) ([]*services.Resource, error) {
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
	for _, item := range s.inventory {
		if s.matchesQuery(item.Resource, query) {
			results = append(results, item.Resource)
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

// Reserve attempts to reserve inventory for a transaction
func (s *InventoryService) Reserve(ctx context.Context, id string) (*services.Resource, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.initiated {
		return nil, services.ErrNotInitialized
	}

	if s.closed {
		return nil, services.ErrAlreadyClosed
	}

	item, exists := s.inventory[id]
	if !exists {
		return nil, services.ErrNotFound
	}

	// Check if item can be reserved
	if item.Resource.State != services.StateInitial && 
	   item.Resource.State != services.StateRollback {
		return nil, services.ErrInvalidState
	}

	// Check available quantity
	reservationQty := 1 // Default reservation quantity
	if item.Quantity - item.Reserved < reservationQty {
		return nil, services.NewError("insufficient_stock", "not enough available quantity", nil)
	}

	// Update inventory state
	item.Reserved += reservationQty
	item.Resource.State = services.StateReserved
	item.Resource.Version++
	item.Resource.LastModified = time.Now()

	return item.Resource, nil
}

// Commit finalizes an inventory reservation
func (s *InventoryService) Commit(ctx context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.initiated {
		return services.ErrNotInitialized
	}

	if s.closed {
		return services.ErrAlreadyClosed
	}

	item, exists := s.inventory[id]
	if !exists {
		return services.ErrNotFound
	}

	// Check if item can be committed
	if item.Resource.State != services.StateReserved {
		return services.ErrInvalidState
	}

	// Update inventory state
	item.Quantity -= item.Reserved // Convert reservation to actual reduction
	item.Reserved = 0
	item.Resource.State = services.StateCommitted
	item.Resource.Version++
	item.Resource.LastModified = time.Now()

	// Check if reorder is needed
	if item.Quantity <= item.ReorderPoint {
		// TODO: Emit reorder event
	}

	return nil
}

// Rollback cancels an inventory reservation
func (s *InventoryService) Rollback(ctx context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.initiated {
		return services.ErrNotInitialized
	}

	if s.closed {
		return services.ErrAlreadyClosed
	}

	item, exists := s.inventory[id]
	if !exists {
		return services.ErrNotFound
	}

	// Any state can be rolled back except Confirmed
	if item.Resource.State == services.StateConfirmed {
		return services.ErrInvalidState
	}

	// Update inventory state
	item.Reserved = 0 // Release any reservations
	item.Resource.State = services.StateRollback
	item.Resource.Version++
	item.Resource.LastModified = time.Now()

	return nil
}

// Confirm acknowledges a committed inventory change (for 3PS)
func (s *InventoryService) Confirm(ctx context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.initiated {
		return services.ErrNotInitialized
	}

	if s.closed {
		return services.ErrAlreadyClosed
	}

	item, exists := s.inventory[id]
	if !exists {
		return services.ErrNotFound
	}

	// Check if item can be confirmed
	if item.Resource.State != services.StateCommitted {
		return services.ErrInvalidState
	}

	// Update inventory state
	item.Resource.State = services.StateConfirmed
	item.Resource.Version++
	item.Resource.LastModified = time.Now()

	return nil
}

// CheckStatus verifies inventory state (for 2.5PS)
func (s *InventoryService) CheckStatus(ctx context.Context, id string) (services.State, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if !s.initiated {
		return "", services.ErrNotInitialized
	}

	if s.closed {
		return "", services.ErrAlreadyClosed
	}

	item, exists := s.inventory[id]
	if !exists {
		return "", services.ErrNotFound
	}

	return item.Resource.State, nil
}

// Healthy returns true if the service is operating normally
func (s *InventoryService) Healthy(ctx context.Context) bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return s.initiated && !s.closed
}

// Helper function to check if a resource matches query criteria
func (s *InventoryService) matchesQuery(resource *services.Resource, query *services.ResourceQuery) bool {
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

// Package queue implements core queue benchmarking functionality
package queue

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/ha1tch/microbench/internal/broker"
	"github.com/ha1tch/microbench/internal/metrics"
)

// SubscriberConfig holds configuration for a subscriber
type SubscriberConfig struct {
	// ID uniquely identifies the subscriber
	ID int

	// TopicPattern to subscribe to
	TopicPattern string

	// QueueName for queue subscription (optional)
	QueueName string

	// ProcessingTime simulates message processing
	ProcessingTime time.Duration

	// BatchSize for receiving messages
	BatchSize int

	// BatchTimeout for batch operations
	BatchTimeout time.Duration
}

// Subscriber manages message subscription for benchmarking
type Subscriber struct {
	config  *SubscriberConfig
	broker  broker.Broker
	metrics *metrics.Collector

	mu           sync.RWMutex
	msgCount     int64
	errorCount   int64
	startTime    time.Time
	lastMsgTime  time.Time
	subscription string
	isRunning    bool
}

// NewSubscriber creates a new subscriber instance
func NewSubscriber(config *SubscriberConfig, b broker.Broker, m *metrics.Collector) *Subscriber {
	return &Subscriber{
		config:  config,
		broker:  b,
		metrics: m,
	}
}

// Start begins receiving messages
func (s *Subscriber) Start(ctx context.Context) error {
	s.mu.Lock()
	if s.isRunning {
		s.mu.Unlock()
		return fmt.Errorf("subscriber already running")
	}
	s.isRunning = true
	s.startTime = time.Now()
	s.mu.Unlock()

	// Create message handler
	handler := func(msg *broker.Message) error {
		// Record message receipt
		s.mu.Lock()
		s.msgCount++
		s.lastMsgTime = time.Now()
		s.mu.Unlock()

		// Record metrics
		s.metrics.RecordMessage(false)
		s.metrics.RecordLatency(msg.Timestamp)

		// Simulate message processing if configured
		if s.config.ProcessingTime > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(s.config.ProcessingTime):
				// Processing complete
			}
		}

		return nil
	}

	// Subscribe to topic
	topic := fmt.Sprintf("%s.%d", s.config.TopicPattern, s.config.ID)
	opts := &broker.SubscribeOptions{
		Queue:        s.config.QueueName,
		BatchSize:    s.config.BatchSize,
		BatchTimeout: s.config.BatchTimeout,
	}

	err := s.broker.Subscribe(ctx, topic, handler, opts)
	if err != nil {
		s.recordError()
		return fmt.Errorf("failed to subscribe to topic %s: %v", topic, err)
	}

	s.mu.Lock()
	s.subscription = topic
	s.mu.Unlock()

	// Wait for context cancellation
	<-ctx.Done()
	return nil
}

// Stop halts message subscription
func (s *Subscriber) Stop(ctx context.Context) error {
	s.mu.Lock()
	if !s.isRunning {
		s.mu.Unlock()
		return nil
	}
	s.isRunning = false
	topic := s.subscription
	s.mu.Unlock()

	// Unsubscribe if we have an active subscription
	if topic != "" {
		if err := s.broker.Unsubscribe(ctx, topic); err != nil {
			return fmt.Errorf("failed to unsubscribe from topic %s: %v", topic, err)
		}
	}

	return nil
}

// GetStats returns current subscriber statistics
func (s *Subscriber) GetStats() map[string]interface{} {
	s.mu.RLock()
	defer s.mu.RUnlock()

	duration := time.Since(s.startTime)
	var lastMsgAge time.Duration
	if !s.lastMsgTime.IsZero() {
		lastMsgAge = time.Since(s.lastMsgTime)
	}

	stats := map[string]interface{}{
		"messages":          s.msgCount,
		"errors":           s.errorCount,
		"duration":         duration,
		"messages_per_s":   float64(s.msgCount) / duration.Seconds(),
		"last_message_age": lastMsgAge,
	}
	return stats
}

// recordError updates error statistics
func (s *Subscriber) recordError() {
	s.mu.Lock()
	s.errorCount++
	s.mu.Unlock()
}

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

// PublisherConfig holds configuration for a publisher
type PublisherConfig struct {
	// ID uniquely identifies the publisher
	ID int

	// TopicPattern for publishing messages
	TopicPattern string

	// MessageSize in bytes
	MessageSize int

	// BatchSize for batch publishing
	BatchSize int

	// PublishInterval between batches
	PublishInterval time.Duration

	// PublishTimeout for each publish operation
	PublishTimeout time.Duration
}

// Publisher manages message publishing for benchmarking
type Publisher struct {
	config  *PublisherConfig
	broker  broker.Broker
	metrics *metrics.Collector

	mu          sync.RWMutex
	msgCount    int64
	batchCount  int64
	errorCount  int64
	retryCount  int64
	startTime   time.Time
	isRunning   bool
}

// NewPublisher creates a new publisher instance
func NewPublisher(config *PublisherConfig, b broker.Broker, m *metrics.Collector) *Publisher {
	return &Publisher{
		config:   config,
		broker:   b,
		metrics:  m,
	}
}

// Start begins publishing messages
func (p *Publisher) Start(ctx context.Context) error {
	p.mu.Lock()
	if p.isRunning {
		p.mu.Unlock()
		return fmt.Errorf("publisher already running")
	}
	p.isRunning = true
	p.startTime = time.Now()
	p.mu.Unlock()

	// Create message payload
	payload := make([]byte, p.config.MessageSize)
	for i := range payload {
		payload[i] = 'x'
	}

	// Start publishing loop
	ticker := time.NewTicker(p.config.PublishInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if err := p.publishBatch(ctx, payload); err != nil {
				// Log error but continue publishing
				p.recordError()
				continue
			}
		}
	}
}

// Stop halts message publishing
func (p *Publisher) Stop() {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.isRunning = false
}

// GetStats returns current publisher statistics
func (p *Publisher) GetStats() map[string]interface{} {
	p.mu.RLock()
	defer p.mu.RUnlock()

	duration := time.Since(p.startTime)
	stats := map[string]interface{}{
		"messages":        p.msgCount,
		"batches":        p.batchCount,
		"errors":         p.errorCount,
		"retries":        p.retryCount,
		"duration":       duration,
		"messages_per_s": float64(p.msgCount) / duration.Seconds(),
	}
	return stats
}

// publishBatch publishes a batch of messages
func (p *Publisher) publishBatch(ctx context.Context, payload []byte) error {
	// Create batch of messages
	batch := make([]*broker.Message, p.config.BatchSize)
	for i := range batch {
		p.mu.Lock()
		msgNum := p.msgCount + int64(i)
		p.mu.Unlock()

		batch[i] = &broker.Message{
			Topic:     fmt.Sprintf("%s.%d", p.config.TopicPattern, p.config.ID),
			Payload:   payload,
			Timestamp: time.Now(),
			Headers: map[string]string{
				"publisher_id": fmt.Sprintf("%d", p.config.ID),
				"msg_num":     fmt.Sprintf("%d", msgNum),
				"batch_size":  fmt.Sprintf("%d", p.config.BatchSize),
			},
		}
	}

	// Publish each message in the batch
	for _, msg := range batch {
		publishCtx, cancel := context.WithTimeout(ctx, p.config.PublishTimeout)
		err := p.broker.Publish(publishCtx, msg, &broker.PublishOptions{
			Timeout: p.config.PublishTimeout,
		})
		cancel()

		if err != nil {
			return fmt.Errorf("failed to publish message: %v", err)
		}

		// Record successful publish
		p.mu.Lock()
		p.msgCount++
		p.mu.Unlock()
		p.metrics.RecordMessage(true)
	}

	// Record successful batch
	p.mu.Lock()
	p.batchCount++
	p.mu.Unlock()

	return nil
}

// recordError updates error statistics
func (p *Publisher) recordError() {
	p.mu.Lock()
	p.errorCount++
	p.mu.Unlock()
}

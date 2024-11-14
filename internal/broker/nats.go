// Package broker implements message broker functionality
package broker

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/nats-io/nats.go"
)

// NATSBroker implements the Broker interface using NATS
type NATSBroker struct {
	conn      *nats.Conn
	config    *Config
	mu        sync.RWMutex
	subs      map[string]*nats.Subscription
	initiated bool
	closed    bool
	metrics   *BrokerMetrics
}

// BrokerMetrics tracks NATS broker performance
type BrokerMetrics struct {
	mu              sync.RWMutex
	messagesSent    int64
	messagesReceived int64
	errors          int64
	reconnects      int64
	disconnects     int64
	roundtripTimes  []time.Duration
}

// NewNATSBroker creates a new NATS broker instance
func NewNATSBroker(config *Config) (*NATSBroker, error) {
	if err := validateNATSConfig(config); err != nil {
		return nil, err
	}

	return &NATSBroker{
		config:  config,
		subs:    make(map[string]*nats.Subscription),
		metrics: &BrokerMetrics{
			roundtripTimes: make([]time.Duration, 0),
		},
	}, nil
}

func validateNATSConfig(config *Config) error {
	if config == nil {
		return ErrInvalidConfig
	}
	if len(config.Addresses) == 0 {
		return NewError("invalid_config", "no NATS addresses provided", nil)
	}
	return nil
}

// Initialize connects to the NATS server
func (b *NATSBroker) Initialize(ctx context.Context) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.initiated {
		return nil
	}

	opts := []nats.Option{
		nats.Name("microbench-client"),
		nats.Timeout(b.config.ConnectionTimeout),
		nats.ReconnectWait(b.config.ReconnectWait),
		nats.MaxReconnects(b.config.MaxReconnects),
		
		// Connection event handlers
		nats.DisconnectErrHandler(func(nc *nats.Conn, err error) {
			b.metrics.mu.Lock()
			b.metrics.disconnects++
			b.metrics.mu.Unlock()
			fmt.Printf("NATS disconnected: %v\n", err)
		}),
		
		nats.ReconnectHandler(func(nc *nats.Conn) {
			b.metrics.mu.Lock()
			b.metrics.reconnects++
			b.metrics.mu.Unlock()
			fmt.Printf("NATS reconnected to %v\n", nc.ConnectedUrl())
		}),
		
		nats.ErrorHandler(func(nc *nats.Conn, sub *nats.Subscription, err error) {
			b.metrics.mu.Lock()
			b.metrics.errors++
			b.metrics.mu.Unlock()
			fmt.Printf("NATS error: %v\n", err)
		}),

		nats.ConnectHandler(func(nc *nats.Conn) {
			fmt.Printf("Connected to NATS server at %s\n", nc.ConnectedUrl())
		}),
	}

	// Connect to NATS
	nc, err := nats.Connect(b.config.Addresses[0], opts...)
	if err != nil {
		return NewError("connection_failed", "failed to connect to NATS", err)
	}

	b.conn = nc
	b.initiated = true
	return nil
}

// Close disconnects from NATS
func (b *NATSBroker) Close(ctx context.Context) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.closed {
		return ErrAlreadyClosed
	}

	if !b.initiated {
		return ErrNotInitialized
	}

	// Unsubscribe from all topics
	for topic, sub := range b.subs {
		if err := sub.Unsubscribe(); err != nil {
			fmt.Printf("Error unsubscribing from %s: %v\n", topic, err)
		}
		delete(b.subs, topic)
	}

	b.conn.Close()
	b.closed = true
	return nil
}

// Publish sends a message to NATS
func (b *NATSBroker) Publish(ctx context.Context, msg *Message, opts *PublishOptions) error {
	b.mu.RLock()
	defer b.mu.RUnlock()

	if !b.initiated {
		return ErrNotInitialized
	}

	if b.closed {
		return ErrAlreadyClosed
	}

	// Convert headers to NATS header
	header := nats.Header{}
	for k, v := range msg.Headers {
		header.Add(k, v)
	}

	// Create NATS message
	natsMsg := &nats.Msg{
		Subject: msg.Topic,
		Data:    msg.Payload,
		Header:  header,
	}

	// Track start time for metrics
	start := time.Now()

	// Publish with timeout if specified
	var err error
	if opts != nil && opts.Timeout > 0 {
		err = b.conn.PublishMsg(natsMsg)
	} else {
		err = b.conn.PublishMsg(natsMsg)
	}

	// Update metrics
	b.metrics.mu.Lock()
	if err == nil {
		b.metrics.messagesSent++
		b.metrics.roundtripTimes = append(b.metrics.roundtripTimes, time.Since(start))
	} else {
		b.metrics.errors++
	}
	b.metrics.mu.Unlock()

	if err != nil {
		return NewError("publish_failed", "failed to publish message", err)
	}

	return nil
}

// Subscribe registers a message handler for a topic
func (b *NATSBroker) Subscribe(ctx context.Context, topic string, handler MessageHandler, opts *SubscribeOptions) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if !b.initiated {
		return ErrNotInitialized
	}

	if b.closed {
		return ErrAlreadyClosed
	}

	// Check if already subscribed
	if _, exists := b.subs[topic]; exists {
		return NewError("already_subscribed", fmt.Sprintf("already subscribed to topic: %s", topic), nil)
	}

	// Create message handler
	msgHandler := func(natsMsg *nats.Msg) {
		// Track message receipt in metrics
		b.metrics.mu.Lock()
		b.metrics.messagesReceived++
		b.metrics.mu.Unlock()

		msg := &Message{
			Topic:     natsMsg.Subject,
			Payload:   natsMsg.Data,
			Timestamp: time.Now(),
			Headers:   make(map[string]string),
			ID:        natsMsg.Reply,
		}

		// Convert NATS headers to message headers
		for k, v := range natsMsg.Header {
			if len(v) > 0 {
				msg.Headers[k] = v[0]
			}
		}

		if err := handler(msg); err != nil {
			b.metrics.mu.Lock()
			b.metrics.errors++
			b.metrics.mu.Unlock()
			fmt.Printf("Error handling message: %v\n", err)
		}
	}

	var sub *nats.Subscription
	var err error

	// Subscribe based on options
	if opts != nil && opts.Queue != "" {
		sub, err = b.conn.QueueSubscribe(topic, opts.Queue, msgHandler)
	} else {
		sub, err = b.conn.Subscribe(topic, msgHandler)
	}

	if err != nil {
		return NewError("subscribe_failed", fmt.Sprintf("failed to subscribe to topic: %s", topic), err)
	}

	b.subs[topic] = sub
	return nil
}

// Unsubscribe removes a subscription
func (b *NATSBroker) Unsubscribe(ctx context.Context, topic string) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if !b.initiated {
		return ErrNotInitialized
	}

	if b.closed {
		return ErrAlreadyClosed
	}

	sub, exists := b.subs[topic]
	if !exists {
		return NewError("not_subscribed", fmt.Sprintf("not subscribed to topic: %s", topic), nil)
	}

	if err := sub.Unsubscribe(); err != nil {
		return NewError("unsubscribe_failed", fmt.Sprintf("failed to unsubscribe from topic: %s", topic), err)
	}

	delete(b.subs, topic)
	return nil
}

// GetMetrics returns current broker metrics
func (b *NATSBroker) GetMetrics() *BrokerMetrics {
	b.metrics.mu.RLock()
	defer b.metrics.mu.RUnlock()

	// Return a copy to avoid concurrent access issues
	metrics := &BrokerMetrics{
		messagesSent:     b.metrics.messagesSent,
		messagesReceived: b.metrics.messagesReceived,
		errors:          b.metrics.errors,
		reconnects:      b.metrics.reconnects,
		disconnects:     b.metrics.disconnects,
	}

	// Copy roundtrip times
	metrics.roundtripTimes = make([]time.Duration, len(b.metrics.roundtripTimes))
	copy(metrics.roundtripTimes, b.metrics.roundtripTimes)

	return metrics
}

// Healthy returns true if the broker is operating normally
func (b *NATSBroker) Healthy(ctx context.Context) bool {
	b.mu.RLock()
	defer b.mu.RUnlock()

	return b.initiated && !b.closed && b.conn.IsConnected()
}

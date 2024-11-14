// Package broker provides message broker abstractions for the microbench tool
package broker

import (
	"context"
	"time"
)

// Message represents a message in the system
type Message struct {
	// Topic is the message topic/subject
	Topic string

	// Payload is the message content
	Payload []byte

	// Timestamp when the message was created
	Timestamp time.Time

	// Headers contain any message metadata
	Headers map[string]string

	// ID is a unique message identifier
	ID string
}

// SubscribeOptions configures how messages are received
type SubscribeOptions struct {
	// Queue name for queue subscription (optional)
	Queue string

	// BatchSize for receiving messages in batches (0 = no batching)
	BatchSize int

	// Timeout for batch operations
	BatchTimeout time.Duration
}

// PublishOptions configures how messages are published
type PublishOptions struct {
	// Timeout for publish operation
	Timeout time.Duration
}

// MessageHandler processes received messages
type MessageHandler func(msg *Message) error

// Broker defines the interface that must be implemented by message brokers
type Broker interface {
	// Initialize prepares the broker for operation
	Initialize(ctx context.Context) error

	// Close cleanly shuts down the broker
	Close(ctx context.Context) error

	// Publish sends a message to the specified topic
	Publish(ctx context.Context, msg *Message, opts *PublishOptions) error

	// Subscribe registers a handler for messages on a topic
	Subscribe(ctx context.Context, topic string, handler MessageHandler, opts *SubscribeOptions) error

	// Unsubscribe removes a subscription
	Unsubscribe(ctx context.Context, topic string) error

	// Healthy returns true if the broker is operating normally
	Healthy(ctx context.Context) bool
}

// Config holds broker configuration
type Config struct {
	// Addresses of broker nodes
	Addresses []string

	// ConnectionTimeout for broker operations
	ConnectionTimeout time.Duration

	// ReconnectWait time between reconnection attempts
	ReconnectWait time.Duration

	// MaxReconnects before giving up (-1 for infinite)
	MaxReconnects int
}

// BrokerType identifies the type of message broker
type BrokerType string

const (
	// BrokerNATS identifies the NATS message broker
	BrokerNATS BrokerType = "nats"

	// BrokerKafka identifies the Kafka message broker (future)
	BrokerKafka BrokerType = "kafka"

	// BrokerRabbitMQ identifies the RabbitMQ message broker (future)
	BrokerRabbitMQ BrokerType = "rabbitmq"
)

// Common errors
var (
	ErrNotInitialized    = Error{Code: "broker_not_initialized", Message: "broker not initialized"}
	ErrAlreadyClosed     = Error{Code: "broker_already_closed", Message: "broker already closed"}
	ErrPublishFailed     = Error{Code: "publish_failed", Message: "failed to publish message"}
	ErrSubscribeFailed   = Error{Code: "subscribe_failed", Message: "failed to subscribe to topic"}
	ErrUnsubscribeFailed = Error{Code: "unsubscribe_failed", Message: "failed to unsubscribe from topic"}
	ErrInvalidConfig     = Error{Code: "invalid_config", Message: "invalid broker configuration"}
)

// Error represents a broker error
type Error struct {
	Code    string // Machine-readable error code
	Message string // Human-readable error message
	Cause   error  // Underlying error if any
}

func (e Error) Error() string {
	if e.Cause != nil {
		return e.Message + ": " + e.Cause.Error()
	}
	return e.Message
}

// NewError creates a new broker error
func NewError(code string, message string, cause error) Error {
	return Error{
		Code:    code,
		Message: message,
		Cause:   cause,
	}
}

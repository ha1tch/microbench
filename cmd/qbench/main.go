// Package main implements the queue benchmarking tool
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"time"

	"github.com/ha1tch/microbench/internal/broker"
	"github.com/ha1tch/microbench/internal/metrics"
	"github.com/ha1tch/microbench/internal/queue"
)

// Command line flags
var (
	brokerType     string
	brokerAddress  string
	testDuration   time.Duration
	msgSize        int
	numPublishers  int
	numSubscribers int
	batchSize      int
	queueName      string
	topicPattern   string
	processTime    time.Duration
	publishInterval time.Duration
	publishTimeout time.Duration
)

func init() {
	flag.StringVar(&brokerType, "broker", "nats", "Broker type (nats)")
	flag.StringVar(&brokerAddress, "address", "nats://localhost:4222", "Broker address")
	flag.DurationVar(&testDuration, "duration", 1*time.Minute, "Test duration")
	flag.IntVar(&msgSize, "msgsize", 1024, "Message size in bytes")
	flag.IntVar(&numPublishers, "publishers", 1, "Number of publishers")
	flag.IntVar(&numSubscribers, "subscribers", 1, "Number of subscribers")
	flag.IntVar(&batchSize, "batchsize", 100, "Batch size for publishers and subscribers")
	flag.StringVar(&queueName, "queue", "", "Queue name for queue subscribers")
	flag.StringVar(&topicPattern, "topic", "microbench", "Base topic pattern for publishing/subscribing")
	flag.DurationVar(&processTime, "processtime", 0, "Simulated message processing time")
	flag.DurationVar(&publishInterval, "pubinterval", 100*time.Millisecond, "Interval between publish batches")
	flag.DurationVar(&publishTimeout, "pubtimeout", 1*time.Second, "Publish operation timeout")
}

func main() {
	flag.Parse()

	// Initialize metrics collector
	metrics := metrics.NewCollector()

	// Create broker configuration
	brokerConfig := &broker.Config{
		Addresses:         []string{brokerAddress},
		ConnectionTimeout: 10 * time.Second,
		ReconnectWait:    1 * time.Second,
		MaxReconnects:    -1,
	}

	// Create and initialize broker
	b, err := broker.NewNATSBroker(brokerConfig)
	if err != nil {
		log.Fatalf("Failed to create broker: %v", err)
	}

	ctx := context.Background()
	if err := b.Initialize(ctx); err != nil {
		log.Fatalf("Failed to initialize broker: %v", err)
	}
	defer b.Close(ctx)

	// Create cancellation context
	benchCtx, cancel := context.WithTimeout(ctx, testDuration)
	defer cancel()

	// Setup signal handling
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt)
	go func() {
		<-sigChan
		fmt.Println("\nCancelling benchmark...")
		cancel()
	}()

	// Run the benchmark
	fmt.Printf("Starting benchmark with:\n"+
		"- Broker: %s\n"+
		"- Publishers: %d\n"+
		"- Subscribers: %d\n"+
		"- Message size: %d bytes\n"+
		"- Batch size: %d\n"+
		"- Process time: %v\n"+
		"- Duration: %v\n",
		brokerType, numPublishers, numSubscribers, msgSize, batchSize, 
		processTime, testDuration)

	if err := runBenchmark(benchCtx, b, metrics); err != nil {
		log.Fatalf("Benchmark failed: %v", err)
	}

	// Print results
	printResults(metrics)
}

func runBenchmark(ctx context.Context, b broker.Broker, m *metrics.Collector) error {
	var wg sync.WaitGroup
	errChan := make(chan error, numPublishers+numSubscribers)

	// Start subscribers first
	subscribers := make([]*queue.Subscriber, numSubscribers)
	for i := 0; i < numSubscribers; i++ {
		subConfig := &queue.SubscriberConfig{
			ID:            i,
			TopicPattern:  topicPattern,
			QueueName:     queueName,
			ProcessingTime: processTime,
			BatchSize:     batchSize,
			BatchTimeout:  publishTimeout,
		}

		subscribers[i] = queue.NewSubscriber(subConfig, b, m)
		wg.Add(1)
		go func(sub *queue.Subscriber) {
			defer wg.Done()
			if err := sub.Start(ctx); err != nil {
				errChan <- fmt.Errorf("subscriber error: %v", err)
			}
		}(subscribers[i])
	}

	// Wait for subscribers to initialize
	time.Sleep(1 * time.Second)

	// Start publishers
	publishers := make([]*queue.Publisher, numPublishers)
	for i := 0; i < numPublishers; i++ {
		pubConfig := &queue.PublisherConfig{
			ID:              i,
			TopicPattern:    topicPattern,
			MessageSize:     msgSize,
			BatchSize:       batchSize,
			PublishInterval: publishInterval,
			PublishTimeout:  publishTimeout,
		}

		publishers[i] = queue.NewPublisher(pubConfig, b, m)
		wg.Add(1)
		go func(pub *queue.Publisher) {
			defer wg.Done()
			if err := pub.Start(ctx); err != nil {
				errChan <- fmt.Errorf("publisher error: %v", err)
			}
		}(publishers[i])
	}

	// Wait for completion or error
	go func() {
		wg.Wait()
		close(errChan)
	}()

	// Check for errors
	for err := range errChan {
		if err != nil {
			return err
		}
	}

	// Clean shutdown of subscribers
	for _, sub := range subscribers {
		if err := sub.Stop(ctx); err != nil {
			fmt.Printf("Warning: failed to stop subscriber: %v\n", err)
		}
	}

	return nil
}

func printResults(m *metrics.Collector) {
	stats := m.GetOverallStats()
	fmt.Printf("\nBenchmark Results:\n")
	fmt.Printf("Messages:\n")
	fmt.Printf("  Sent:     %d\n", stats["messages_sent"])
	fmt.Printf("  Received: %d\n", stats["messages_received"])
	fmt.Printf("  Loss:     %.2f%%\n", 
		100.0*(1.0-float64(stats["messages_received"].(int64))/float64(stats["messages_sent"].(int64))))

	fmt.Printf("\nLatency:\n")
	fmt.Printf("  Average: %v\n", stats["latency_avg"])
	fmt.Printf("  P95:     %v\n", stats["latency_p95"])
	fmt.Printf("  P99:     %v\n", stats["latency_p99"])

	fmt.Printf("\nThroughput:\n")
	duration := stats["duration"].(time.Duration)
	fmt.Printf("  Duration:    %v\n", duration)
	fmt.Printf("  Messages/s:  %.2f\n", 
		float64(stats["messages_sent"].(int64))/duration.Seconds())
	fmt.Printf("  Throughput:  %.2f MB/s\n", 
		float64(stats["messages_sent"].(int64)*int64(msgSize))/(duration.Seconds()*1024*1024))

	if stats["error_count"].(int64) > 0 {
		fmt.Printf("\nErrors: %d\n", stats["error_count"])
	}
}

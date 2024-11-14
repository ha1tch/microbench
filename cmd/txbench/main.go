// Package main implements the transaction pattern benchmarking tool
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"sync"
	"time"

	"github.com/ha1tch/microbench/internal/broker"
	"github.com/ha1tch/microbench/internal/patterns"
	"github.com/ha1tch/microbench/internal/services"
	"github.com/ha1tch/microbench/internal/txbench"
)

// Command line flags
var (
	brokerType     string
	brokerAddress  string
	testDuration   time.Duration
	patternNames   string
	concurrency    int
	failureRate    float64
	txTimeout      time.Duration
	warmupDuration time.Duration
	cooldownDelay  time.Duration
	retryAttempts  int
	retryDelay     time.Duration
)

// Pattern configurations
var (
	patternConfigs = map[string]func() patterns.Pattern{
		"2pc": func() patterns.Pattern {
			return patterns.NewTwoPhaseCommit(&patterns.Config{
				PrepareTimeout:  txTimeout,
				CommitTimeout:   txTimeout,
				RollbackTimeout: txTimeout,
				RetryAttempts:  retryAttempts,
				RetryDelay:     retryDelay,
			})
		},
		"saga": func() patterns.Pattern {
			return patterns.NewSaga(&patterns.Config{
				PrepareTimeout:  txTimeout,
				CommitTimeout:   txTimeout,
				RollbackTimeout: txTimeout,
				RetryAttempts:  retryAttempts,
				RetryDelay:     retryDelay,
			})
		},
		"2ps": func() patterns.Pattern {
			return patterns.NewTwoPhaseSaga(&patterns.Config{
				PrepareTimeout:  txTimeout,
				CommitTimeout:   txTimeout,
				RollbackTimeout: txTimeout,
				RetryAttempts:  retryAttempts,
				RetryDelay:     retryDelay,
			})
		},
		"3ps": func() patterns.Pattern {
			return patterns.NewThreePhaseSaga(&patterns.Config{
				PrepareTimeout:  txTimeout,
				CommitTimeout:   txTimeout,
				RollbackTimeout: txTimeout,
				ConfirmTimeout:  txTimeout,
				RetryAttempts:  retryAttempts,
				RetryDelay:     retryDelay,
			})
		},
		"2.5ps": func() patterns.Pattern {
			p := patterns.NewTwoAndHalfPhaseSaga(&patterns.Config{
				PrepareTimeout:  txTimeout,
				CommitTimeout:   txTimeout,
				RollbackTimeout: txTimeout,
				RetryAttempts:  retryAttempts,
				RetryDelay:     retryDelay,
			})
			p.SetStatusCheckConfig(struct {
				Enabled              bool
				ParticipantThreshold int
				MaxCheckAttempts     int
				RequiredStates       []services.State
			}{
				Enabled:              true,
				ParticipantThreshold: 0,
				MaxCheckAttempts:     3,
				RequiredStates:       []services.State{services.StateReserved},
			})
			return p
		},
	}
)

func init() {
	flag.StringVar(&brokerType, "broker", "nats", "Broker type (nats)")
	flag.StringVar(&brokerAddress, "address", "nats://localhost:4222", "Broker address")
	flag.DurationVar(&testDuration, "duration", 5*time.Minute, "Test duration")
	flag.StringVar(&patternNames, "patterns", "all", "Patterns to test (comma-separated: 2pc,saga,2ps,3ps,2.5ps)")
	flag.IntVar(&concurrency, "concurrency", 10, "Number of concurrent transactions")
	flag.Float64Var(&failureRate, "failures", 0.1, "Simulated failure rate (0.0-1.0)")
	flag.DurationVar(&txTimeout, "timeout", 5*time.Second, "Transaction timeout")
	flag.DurationVar(&warmupDuration, "warmup", 30*time.Second, "Warm-up duration before measuring")
	flag.DurationVar(&cooldownDelay, "cooldown", 5*time.Second, "Cool-down delay after each pattern")
	flag.IntVar(&retryAttempts, "retries", 3, "Number of retry attempts (-1 for infinite)")
	flag.DurationVar(&retryDelay, "retrydelay", 100*time.Millisecond, "Delay between retries")
}

func main() {
	flag.Parse()

	// Setup patterns to test
	patterns := setupPatternsToTest()

	// Create and initialize broker
	b := setupBroker()
	defer cleanupBroker(b)

	// Create benchmark context with cancellation
	benchCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Setup signal handling
	handleSignals(cancel)

	// Run benchmarks for each pattern
	results := runBenchmarks(benchCtx, b, patterns)

	// Print results
	printResults(results)
}

func setupPatternsToTest() []string {
	var patternsToTest []string
	if patternNames == "all" {
		patternsToTest = []string{"2pc", "saga", "2ps", "3ps", "2.5ps"}
	} else {
		patternsToTest = strings.Split(patternNames, ",")
	}

	// Validate patterns
	for _, name := range patternsToTest {
		if _, exists := patternConfigs[name]; !exists {
			log.Fatalf("Unknown pattern: %s", name)
		}
	}

	return patternsToTest
}

func setupBroker() broker.Broker {
	config := &broker.Config{
		Addresses:         []string{brokerAddress},
		ConnectionTimeout: 10 * time.Second,
		ReconnectWait:    1 * time.Second,
		MaxReconnects:    -1,
	}

	b, err := broker.NewNATSBroker(config)
	if err != nil {
		log.Fatalf("Failed to create broker: %v", err)
	}

	if err := b.Initialize(context.Background()); err != nil {
		log.Fatalf("Failed to initialize broker: %v", err)
	}

	return b
}

func cleanupBroker(b broker.Broker) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := b.Close(ctx); err != nil {
		log.Printf("Warning: Failed to close broker cleanly: %v", err)
	}
}

func handleSignals(cancel context.CancelFunc) {
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt)
	go func() {
		<-sigChan
		fmt.Println("\nReceived interrupt signal. Cleaning up...")
		cancel()
	}()
}

func runBenchmarks(ctx context.Context, b broker.Broker, patternsToTest []string) map[string]*txbench.Results {
	results := make(map[string]*txbench.Results)
	var wg sync.WaitGroup

	// Create benchmark configuration
	config := &txbench.Config{
		Broker:         b,
		Concurrency:    concurrency,
		FailureRate:    failureRate,
		Timeout:        txTimeout,
		WarmupDuration: warmupDuration,
	}

	// Run benchmarks for each pattern
	for _, name := range patternsToTest {
		fmt.Printf("\nBenchmarking %s pattern...\n", name)
		pattern := patternConfigs[name]()

		// Create benchmark instance
		bench := txbench.NewBenchmark(config, pattern)

		// Run benchmark in its own goroutine
		wg.Add(1)
		go func(patternName string) {
			defer wg.Done()

			result, err := bench.Run(ctx)
			if err != nil {
				log.Printf("Benchmark failed for %s: %v", patternName, err)
				return
			}
			
			results[patternName] = result

			// Cool down between patterns
			time.Sleep(cooldownDelay)
		}(name)

		wg.Wait() // Wait for pattern to complete before starting next one
	}

	return results
}

func printResults(results map[string]*txbench.Results) {
	fmt.Println("\nBenchmark Results:")
	fmt.Println("==================")

	for pattern, result := range results {
		fmt.Printf("\n%s Pattern:\n", pattern)
		
		// Transaction statistics
		fmt.Printf("  Transactions:\n")
		printTransactionStats(result)

		// Latency statistics
		fmt.Printf("  Latency:\n")
		printLatencyStats(result)

		// Resource usage
		fmt.Printf("  Resource Usage:\n")
		printResourceStats(result)

		// Phase latencies
		if len(result.PhaseLatencies) > 0 {
			fmt.Printf("  Phase Latencies:\n")
			printPhaseStats(result)
		}
	}
}

func printTransactionStats(result *txbench.Results) {
	total := float64(result.TotalTransactions)
	if total == 0 {
		total = 1 // Avoid division by zero
	}

	fmt.Printf("    Total:      %d\n", result.TotalTransactions)
	fmt.Printf("    Successful: %d (%.2f%%)\n",
		result.SuccessfulTransactions,
		float64(result.SuccessfulTransactions)/total*100)
	fmt.Printf("    Failed:     %d (%.2f%%)\n",
		result.FailedTransactions,
		float64(result.FailedTransactions)/total*100)
}

func printLatencyStats(result *txbench.Results) {
	fmt.Printf("    Average: %v\n", result.AverageLatency)
	fmt.Printf("    Median:  %v\n", result.MedianLatency)
	fmt.Printf("    P95:     %v\n", result.P95Latency)
	fmt.Printf("    P99:     %v\n", result.P99Latency)
}

func printResourceStats(result *txbench.Results) {
	fmt.Printf("    Messages:   %d\n", result.MessageCount)
	fmt.Printf("    Retries:    %d\n", result.RetryCount)
	fmt.Printf("    Msg/Tx:     %.2f\n",
		float64(result.MessageCount)/float64(result.TotalTransactions))
}

func printPhaseStats(result *txbench.Results) {
	phases := []string{"prepare", "commit", "confirm", "rollback"}
	for _, phase := range phases {
		if latency, exists := result.PhaseLatencies[phase]; exists {
			fmt.Printf("    %-12s %v\n", phase+":", latency)
		}
	}
}


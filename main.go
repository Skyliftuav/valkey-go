package valkeygo

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/josh-tracey/scribe"
	"github.com/redis/go-redis/v9"
)

// ValkeyMessage implements the message interface expected by observers
type ValkeyMessage struct {
	Topic   string
	Payload string
}

// GetPayloadAsString returns the message payload as string
func (m *ValkeyMessage) GetPayloadAsString() (string, bool) {
	return m.Payload, true
}

// GetPayloadAsBytes returns the message payload as bytes
func (m *ValkeyMessage) GetPayloadAsBytes() ([]byte, bool) {
	return []byte(m.Payload), true
}

// GetDestinationName returns the topic/channel name
func (m *ValkeyMessage) GetDestinationName() string {
	return m.Topic
}

type PubMessage struct {
	Topic   string
	Payload []byte
}

func (m *PubMessage) GetPayloadAsBytes() ([]byte, bool) {
	return []byte(m.Payload), true
}

func (m *PubMessage) GetPayloadAsString() (string, bool) {
	return string(m.Payload), true
}

func (m *PubMessage) GetPayloadAsJSON() (map[string]any, bool) {
	var data map[string]any
	err := json.Unmarshal(m.Payload, &data)
	if err != nil {
		return nil, false
	}
	return data, true
}

func (m *PubMessage) GetDestinationName() string {
	return m.Topic
}

type Observer struct {
	Notify chan *ValkeyMessage
}

type SubMessage struct {
	Topic    string
	Observer Observer
}

// Subscriber is the interface for subscribing to topics.
type Subscriber interface {
	Connect()
	Disconnect()
	Subscribe(topic string, observer Observer) string
	Unsubscribe(subID string)
	Run(ctx context.Context)
}

type CloudEvent struct {
	ID          string `json:"id"`
	Source      string `json:"source"`
	Type        string `json:"type"`
	SpecVersion string `json:"specversion"`
	Data        any    `json:"data"`
	Subject     string `json:"subject"`
	Time        string `json:"time"`
}

func NewCloudEvent(source, t, subject, time string, data any) *CloudEvent {
	return &CloudEvent{
		ID:          uuid.New().String(),
		Source:      source,
		Type:        t,
		SpecVersion: "1.0",
		Data:        data,
		Subject:     subject,
		Time:        time,
	}
}

func GetEnv(key, def string) string {
	if val, ok := os.LookupEnv(key); ok {
		return val
	}
	return def
}

func GetEnvAsBool(key string, def bool) bool {
	if val, ok := os.LookupEnv(key); ok {
		if val == "true" || val == "1" {
			return true
		} else if val == "false" || val == "0" {
			return false
		}
	}
	return def
}

func GetEnvAsInt(key string, def int) int {
	if val, ok := os.LookupEnv(key); ok {
		if intVal, err := strconv.Atoi(val); err == nil {
			return intVal
		}
	}
	return def
}

// Subscription struct with an ID and context for cancellation
type Subscription struct {
	ID       string
	Topic    string
	Cancel   context.CancelFunc
	Observer Observer
}

// ValkeyAdapter is a struct that contains the Valkey connection details
type ValkeyAdapter struct {
	client            *redis.Client
	pubClient         *redis.Client // Separate client for publishing
	publishQueue      chan *PubMessage
	logger            *scribe.Logger
	subscriptions     map[string]*Subscription // Map to store subscriptions by ID
	subscriptionsMux  sync.RWMutex             // Mutex for thread-safe subscription management
	ctx               context.Context
	cancel            context.CancelFunc
	reconnectAttempts int
	maxReconnectDelay time.Duration

	// --- NEW ---
	// Configuration for the subscription worker pool.
	subWorkerPoolSize int // Number of workers per subscription
	subJobQueueSize   int // Size of the job queue for each subscription's worker pool
}

// ValkeyConfig holds configuration for Valkey connection
type ValkeyConfig struct {
	Host         string
	Port         string
	Password     string
	Database     int
	PoolSize     int
	MinIdleConns int
	MaxRetries   int
	DialTimeout  time.Duration
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
}

// NewValkeyAdapter creates a new Valkey adapter instance
func NewValkeyAdapter(publishQueue chan *PubMessage, logger *scribe.Logger) *ValkeyAdapter {
	config := &ValkeyConfig{
		Host:         GetEnv("VALKEY_HOST", "localhost"),
		Port:         GetEnv("VALKEY_PORT", "6379"),
		Password:     GetEnv("VALKEY_PASSWORD", ""),
		Database:     GetEnvAsInt("VALKEY_DATABASE", 0),
		PoolSize:     GetEnvAsInt("VALKEY_POOL_SIZE", 10),
		MinIdleConns: GetEnvAsInt("VALKEY_MIN_IDLE_CONNS", 5),
		MaxRetries:   GetEnvAsInt("VALKEY_MAX_RETRIES", 3),
		DialTimeout:  time.Duration(GetEnvAsInt("VALKEY_DIAL_TIMEOUT", 5)) * time.Second,
		ReadTimeout:  time.Duration(GetEnvAsInt("VALKEY_READ_TIMEOUT", 3)) * time.Second,
		WriteTimeout: time.Duration(GetEnvAsInt("VALKEY_WRITE_TIMEOUT", 3)) * time.Second,
	}

	logger.Debug("Valkey Host: %s", config.Host)
	logger.Debug("Valkey Port: %s", config.Port)
	logger.Debug("Valkey Database: %d", config.Database)
	logger.Debug("Valkey Pool Size: %d", config.PoolSize)

	ctx, cancel := context.WithCancel(context.Background())

	return &ValkeyAdapter{
		publishQueue:      publishQueue,
		logger:            logger,
		subscriptions:     make(map[string]*Subscription),
		ctx:               ctx,
		cancel:            cancel,
		maxReconnectDelay: 30 * time.Second,
		client:            createValkeyClient(config),
		pubClient:         createValkeyClient(config),
		subWorkerPoolSize: GetEnvAsInt("VALKEY_SUB_WORKER_POOL_SIZE", 10),
		subJobQueueSize:   GetEnvAsInt("VALKEY_SUB_JOB_QUEUE_SIZE", 256),
	}
}

// createValkeyClient creates a Valkey client with the given configuration
func createValkeyClient(config *ValkeyConfig) *redis.Client {
	return redis.NewClient(&redis.Options{
		Addr:         fmt.Sprintf("%s:%s", config.Host, config.Port),
		Password:     config.Password,
		DB:           config.Database,
		PoolSize:     config.PoolSize,
		MinIdleConns: config.MinIdleConns,
		MaxRetries:   config.MaxRetries,
		DialTimeout:  config.DialTimeout,
		ReadTimeout:  config.ReadTimeout,
		WriteTimeout: config.WriteTimeout,
	})
}

// Connect establishes connection to Valkey/Valkey server
func (r *ValkeyAdapter) Connect() error {
	// Test connection with ping
	ctx, cancel := context.WithTimeout(r.ctx, 5*time.Second)
	defer cancel()

	pong, err := r.client.Ping(ctx).Result()
	if err != nil {
		r.logger.Error("Failed to connect to Valkey: %v", err)
		return fmt.Errorf("failed to connect to Valkey: %w", err)
	}

	r.logger.Info("Connected to Valkey: %s", pong)

	// Test pub client connection
	pong, err = r.pubClient.Ping(ctx).Result()
	if err != nil {
		r.logger.Error("Failed to connect Valkey pub client: %v", err)
		return fmt.Errorf("failed to connect Valkey pub client: %w", err)
	}

	// Start connection monitoring
	go r.monitorConnection()

	return nil
}

// monitorConnection monitors Valkey connection health and handles reconnection
func (r *ValkeyAdapter) monitorConnection() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-r.ctx.Done():
			return
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(r.ctx, 5*time.Second)
			err := r.client.Ping(ctx).Err()
			cancel()

			if err != nil {
				r.logger.Error("Valkey connection lost: %v", err)
				r.handleReconnection()
			}
		}
	}
}

// handleReconnection implements exponential backoff reconnection strategy
func (r *ValkeyAdapter) handleReconnection() {
	r.reconnectAttempts++
	delay := time.Duration(r.reconnectAttempts) * time.Second
	if delay > r.maxReconnectDelay {
		delay = r.maxReconnectDelay
	}

	r.logger.Info("Attempting to reconnect to Valkey in %v (attempt %d)", delay, r.reconnectAttempts)
	time.Sleep(delay)

	if err := r.Connect(); err != nil {
		r.logger.Error("Reconnection failed: %v", err)
		go r.handleReconnection() // Retry
	} else {
		r.logger.Info("Successfully reconnected to Valkey")
		r.reconnectAttempts = 0
		r.resubscribeAll()
	}
}

// --- MODIFIED ---
// resubscribeAll now correctly creates and passes new contexts for each subscription.
func (r *ValkeyAdapter) resubscribeAll() {
	r.subscriptionsMux.RLock()
	subscriptions := make([]*Subscription, 0, len(r.subscriptions))
	for _, sub := range r.subscriptions {
		subscriptions = append(subscriptions, sub)
	}
	r.subscriptionsMux.RUnlock()

	for _, sub := range subscriptions {
		r.logger.Info("Resubscribing to topic: %s", sub.Topic)
		// Create a new context for the new subscription instance
		subCtx, cancel := context.WithCancel(r.ctx)
		sub.Cancel = cancel // Update the subscription with the new cancel function
		go r.startSubscription(subCtx, sub)
	}
}

// --- MODIFIED ---
// Subscribe now creates a dedicated context for each subscription for proper cancellation.
func (r *ValkeyAdapter) Subscribe(topic string, observer Observer) string {
	subscriptionID := uuid.New().String()

	// Create a context that is specific to THIS subscription.
	// This allows individual cancellation via Unsubscribe.
	subCtx, cancel := context.WithCancel(r.ctx)

	subscription := &Subscription{
		ID:       subscriptionID,
		Topic:    topic,
		Cancel:   cancel, // This cancel function is now tied to subCtx
		Observer: observer,
	}

	r.subscriptionsMux.Lock()
	r.subscriptions[subscriptionID] = subscription
	r.subscriptionsMux.Unlock()

	// Pass the subscription-specific context to the goroutine.
	go r.startSubscription(subCtx, subscription)

	r.logger.Info("Created subscription %s for topic: %s", subscriptionID, topic)
	return subscriptionID
}

// --- NEW ---
// observerWorker is a long-running function that processes messages for a single subscription.
// It receives messages from a jobs channel and sends them to the observer.
func (r *ValkeyAdapter) observerWorker(ctx context.Context, wg *sync.WaitGroup, subscription *Subscription, jobs <-chan *ValkeyMessage) {
	defer wg.Done()
	r.logger.Debug("Starting observer worker for topic %s", subscription.Topic)

	for {
		select {
		case <-ctx.Done():
			// The subscription or the entire adapter is shutting down.
			r.logger.Debug("Observer worker shutting down for topic %s", subscription.Topic)
			return
		case msg, ok := <-jobs:
			if !ok {
				// The jobs channel was closed, meaning no more messages.
				r.logger.Debug("Observer worker shutting down for topic %s (channel closed)", subscription.Topic)
				return
			}

			// This is the potentially slow part. We block here, but only this single worker
			// is blocked. The main `startSubscription` loop and other workers are unaffected.
			select {
			case subscription.Observer.Notify <- msg:
				r.logger.Trace("Message delivered by worker to observer for topic: %s", subscription.Topic)
			case <-ctx.Done():
				// The context was cancelled while we were trying to send.
				return
			}
		}
	}
}

// --- MODIFIED ---
// startSubscription is now a fast dispatcher that hands off messages to a pool of workers.
func (r *ValkeyAdapter) startSubscription(ctx context.Context, subscription *Subscription) {
	pubsub := r.client.Subscribe(ctx, subscription.Topic)
	defer pubsub.Close()

	// Wait for subscription confirmation from Valkey.
	_, err := pubsub.Receive(ctx)
	if err != nil {
		// If context is cancelled, it's a normal shutdown, not an error.
		if ctx.Err() != context.Canceled {
			r.logger.Error("Failed to confirm subscription for topic %s: %v", subscription.Topic, err)
		}
		return
	}

	r.logger.Info("Successfully subscribed to topic: %s", subscription.Topic)

	// --- WORKER POOL SETUP ---
	jobs := make(chan *ValkeyMessage, r.subJobQueueSize)
	var wg sync.WaitGroup

	for i := 0; i < r.subWorkerPoolSize; i++ {
		wg.Add(1)
		go r.observerWorker(ctx, &wg, subscription, jobs)
	}

	// This loop reads from Valkey and dispatches to the jobs channel.
	ch := pubsub.Channel()
	for {
		select {
		case <-ctx.Done():
			// Context was cancelled (e.g., by Unsubscribe or Disconnect).
			close(jobs)
			wg.Wait()
			r.logger.Info("Subscription stopped for topic: %s", subscription.Topic)
			return
		case msg, ok := <-ch:
			if !ok {
				// The Valkey channel was closed.
				r.logger.Warn("Subscription channel closed by Valkey for topic: %s", subscription.Topic)
				close(jobs)
				wg.Wait()
				return
			}

			// Non-blocking send to the job queue.
			select {
			case jobs <- &ValkeyMessage{Topic: msg.Channel, Payload: msg.Payload}:
				// Message successfully queued for a worker.
			}
		}
	}
}

// Unsubscribe removes a subscription using the subscription ID
func (r *ValkeyAdapter) Unsubscribe(subscriptionID string) {
	r.subscriptionsMux.Lock()
	subscription, exists := r.subscriptions[subscriptionID]
	if !exists {
		r.subscriptionsMux.Unlock()
		r.logger.Warn("Subscription ID not found: %s", subscriptionID)
		return
	}

	// Remove from map first
	delete(r.subscriptions, subscriptionID)
	r.subscriptionsMux.Unlock()

	// Cancel the subscription context, which will trigger the shutdown of its goroutines.
	subscription.Cancel()

	r.logger.Info("Unsubscribed from topic: %s (ID: %s)", subscription.Topic, subscriptionID)
}

// Publish publishes a message to a topic
func (r *ValkeyAdapter) Publish(topic string, message string) error {
	ctx, cancel := context.WithTimeout(r.ctx, 5*time.Second)
	defer cancel()

	err := r.pubClient.Publish(ctx, topic, message).Err()
	if err != nil {
		r.logger.Error("Failed to publish message to topic %s: %v", topic, err)
		return fmt.Errorf("failed to publish message: %w", err)
	}

	r.logger.Info("Published message to topic: %s", topic)
	return nil
}

// PublishBatch publishes multiple messages in a pipeline for better performance
func (r *ValkeyAdapter) PublishBatch(messages []*PubMessage) error {
	if len(messages) == 0 {
		return nil
	}

	ctx, cancel := context.WithTimeout(r.ctx, 10*time.Second)
	defer cancel()

	pipe := r.pubClient.Pipeline()
	for _, msg := range messages {
		pipe.Publish(ctx, msg.Topic, string(msg.Payload))
	}

	_, err := pipe.Exec(ctx)
	if err != nil {
		r.logger.Error("Failed to publish batch messages: %v", err)
		return fmt.Errorf("failed to publish batch messages: %w", err)
	}

	r.logger.Info("Published %d messages in batch", len(messages))
	return nil
}

// Disconnect closes the Valkey connection
func (r *ValkeyAdapter) Disconnect() error {
	r.logger.Info("Disconnecting from Valkey...")

	// Cancel the main context to stop all derived contexts and goroutines.
	r.cancel()

	// The individual subscription goroutines will shut down gracefully.
	// We can clear the map.
	r.subscriptionsMux.Lock()
	r.subscriptions = make(map[string]*Subscription)
	r.subscriptionsMux.Unlock()

	// Close Valkey clients
	var firstErr error
	if err := r.client.Close(); err != nil {
		r.logger.Error("Error closing Valkey client: %v", err)
		firstErr = err
	}

	if err := r.pubClient.Close(); err != nil {
		r.logger.Error("Error closing Valkey pub client: %v", err)
		if firstErr == nil {
			firstErr = err
		}
	}

	r.logger.Info("Disconnected from Valkey")
	return firstErr
}

// Run processes the publish queue and handles batch publishing
func (r *ValkeyAdapter) Run(ctx context.Context) {
	batchSize := GetEnvAsInt("VALKEY_BATCH_SIZE", 10)
	batchTimeout := time.Duration(GetEnvAsInt("VALKEY_BATCH_TIMEOUT_MS", 100)) * time.Millisecond

	batch := make([]*PubMessage, 0, batchSize)
	ticker := time.NewTicker(batchTimeout)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			if len(batch) > 0 {
				r.PublishBatch(batch)
			}
			return

		case pubMessage := <-r.publishQueue:
			log.Printf("[VALKEY_ADAPTER_TRACE] Valkey adapter received message from pubQueue for topic '%s'. Batching.", pubMessage.Topic)
			batch = append(batch, pubMessage)
			if len(batch) >= batchSize {
				r.PublishBatch(batch)
				batch = batch[:0]
			}

		case <-ticker.C:
			if len(batch) > 0 {
				r.PublishBatch(batch)
				batch = batch[:0]
			}
		}
	}
}

// GetStats returns connection and subscription statistics
func (r *ValkeyAdapter) GetStats() map[string]any {
	r.subscriptionsMux.RLock()
	subscriptionCount := len(r.subscriptions)
	r.subscriptionsMux.RUnlock()

	stats := r.client.PoolStats()

	return map[string]any{
		"subscriptions":      subscriptionCount,
		"pool_hits":          stats.Hits,
		"pool_misses":        stats.Misses,
		"pool_timeouts":      stats.Timeouts,
		"pool_total_conns":   stats.TotalConns,
		"pool_idle_conns":    stats.IdleConns,
		"pool_stale_conns":   stats.StaleConns,
		"reconnect_attempts": r.reconnectAttempts,
	}
}

// HealthCheck performs a health check on the Valkey connection
func (r *ValkeyAdapter) HealthCheck() error {
	ctx, cancel := context.WithTimeout(r.ctx, 2*time.Second)
	defer cancel()

	return r.client.Ping(ctx).Err()
}

// StreamSubscription holds the details for a Valkey Stream subscription.
type StreamSubscription struct {
	ID            string
	Stream        string
	Group         string
	ConsumerID    string
	Cancel        context.CancelFunc
	Observer      Observer
	wg            sync.WaitGroup
	lastMessageID string
}

// ValkeyStreamsAdapter is a struct that contains the Valkey connection and stream-specific details.
type ValkeyStreamsAdapter struct {
	client              *redis.Client
	pubClient           *redis.Client // Separate client for publishing
	logger              *scribe.Logger
	publishQueue        chan *PubMessage
	subscriptions       map[string]*StreamSubscription
	subscriptionsMux    sync.RWMutex
	ctx                 context.Context
	cancel              context.CancelFunc
	reconnectAttempts   int
	maxReconnectDelay   time.Duration
	workerPoolSize      int
	jobQueueSize        int
	pendingMessageLimit int64
	claimInterval       time.Duration
}

// NewValkeyStreamsAdapter creates a new Valkey Streams adapter instance.
func NewValkeyStreamsAdapter(
	publishQueue chan *PubMessage,
	logger *scribe.Logger) *ValkeyStreamsAdapter {
	config := &ValkeyConfig{
		Host:         GetEnv("VALKEY_HOST", "localhost"),
		Port:         GetEnv("VALKEY_PORT", "6379"),
		Password:     GetEnv("VALKEY_PASSWORD", ""),
		Database:     GetEnvAsInt("VALKEY_DATABASE", 0),
		PoolSize:     GetEnvAsInt("VALKEY_POOL_SIZE", 20),
		MinIdleConns: GetEnvAsInt("VALKEY_MIN_IDLE_CONNS", 10),
		MaxRetries:   GetEnvAsInt("VALKEY_MAX_RETRIES", 5),
		DialTimeout:  time.Duration(GetEnvAsInt("VALKEY_DIAL_TIMEOUT", 5)) * time.Second,
		ReadTimeout:  time.Duration(GetEnvAsInt("VALKEY_READ_TIMEOUT", 3)) * time.Second,
		WriteTimeout: time.Duration(GetEnvAsInt("VALKEY_WRITE_TIMEOUT", 3)) * time.Second,
	}

	logger.Debug("Valkey Streams Host: %s", config.Host)
	logger.Debug("Valkey Streams Port: %s", config.Port)

	ctx, cancel := context.WithCancel(context.Background())

	return &ValkeyStreamsAdapter{
		publishQueue:        publishQueue,
		logger:              logger,
		subscriptions:       make(map[string]*StreamSubscription),
		ctx:                 ctx,
		cancel:              cancel,
		maxReconnectDelay:   60 * time.Second,
		client:              createValkeyClient(config),
		pubClient:           createValkeyClient(config),
		workerPoolSize:      GetEnvAsInt("VALKEY_STREAM_WORKER_POOL_SIZE", 10),
		jobQueueSize:        GetEnvAsInt("VALKEY_STREAM_JOB_QUEUE_SIZE", 256),
		pendingMessageLimit: int64(GetEnvAsInt("VALKEY_STREAM_PENDING_LIMIT", 100)),
		claimInterval:       time.Duration(GetEnvAsInt("VALKEY_STREAM_CLAIM_INTERVAL_SEC", 60)) * time.Second,
	}
}

// Connect establishes a connection to the Valkey server.
func (a *ValkeyStreamsAdapter) Connect() error {
	ctx, cancel := context.WithTimeout(a.ctx, 5*time.Second)
	defer cancel()

	if err := a.client.Ping(ctx).Err(); err != nil {
		a.logger.Error("Failed to connect to Valkey (streams): %v", err)
		return fmt.Errorf("failed to connect to Valkey (streams): %w", err)
	}
	if err := a.pubClient.Ping(ctx).Err(); err != nil {
		a.logger.Error("Failed to connect Valkey pub client (streams): %v", err)
		return fmt.Errorf("failed to connect Valkey pub client (streams): %w", err)
	}

	a.logger.Info("Connected to Valkey for Streams")
	go a.monitorConnection()
	return nil
}

// monitorConnection monitors the Valkey connection and triggers reconnection if needed.
func (a *ValkeyStreamsAdapter) monitorConnection() {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-a.ctx.Done():
			return
		case <-ticker.C:
			if err := a.client.Ping(context.Background()).Err(); err != nil {
				a.logger.Error("Valkey streams connection lost: %v", err)
				a.handleReconnection()
			}
		}
	}
}

// handleReconnection attempts to reconnect to Valkey with exponential backoff.
func (a *ValkeyStreamsAdapter) handleReconnection() {
	a.reconnectAttempts++
	delay := time.Duration(a.reconnectAttempts) * 2 * time.Second
	if delay > a.maxReconnectDelay {
		delay = a.maxReconnectDelay
	}

	a.logger.Info("Attempting to reconnect to Valkey (streams) in %v (attempt %d)", delay, a.reconnectAttempts)
	time.Sleep(delay)

	if err := a.Connect(); err != nil {
		a.logger.Error("Reconnection failed (streams): %v", err)
	} else {
		a.logger.Info("Successfully reconnected to Valkey (streams)")
		a.reconnectAttempts = 0
		a.resubscribeAll()
	}
}

// resubscribeAll re-establishes all active stream subscriptions after a reconnection.
func (a *ValkeyStreamsAdapter) resubscribeAll() {
	a.subscriptionsMux.RLock()
	subsToResubscribe := make([]*StreamSubscription, 0, len(a.subscriptions))
	for _, sub := range a.subscriptions {
		subsToResubscribe = append(subsToResubscribe, sub)
	}
	a.subscriptionsMux.RUnlock()

	for _, sub := range subsToResubscribe {
		a.logger.Info("Resubscribing to stream: %s, group: %s", sub.Stream, sub.Group)
		// Create a new context for the subscription
		subCtx, cancel := context.WithCancel(a.ctx)
		sub.Cancel = cancel
		go a.listenToStream(subCtx, sub)
	}
}

// SubscribeToStream creates a consumer group and starts listening to a stream.
func (a *ValkeyStreamsAdapter) SubscribeToStream(stream, group string, observer Observer) (string, error) {
	subscriptionID := uuid.New().String()
	consumerID := uuid.New().String() // Unique consumer ID for this instance

	// Create the consumer group, ignore error if it already exists
	err := a.client.XGroupCreateMkStream(a.ctx, stream, group, "0").Err()
	if err != nil && !errors.Is(err, redis.Nil) && err.Error() != "BUSYGROUP Consumer Group name already exists" {
		a.logger.Error("Failed to create consumer group for stream %s: %v", stream, err)
		return "", fmt.Errorf("failed to create consumer group: %w", err)
	}

	subCtx, cancel := context.WithCancel(a.ctx)
	subscription := &StreamSubscription{
		ID:            subscriptionID,
		Stream:        stream,
		Group:         group,
		ConsumerID:    consumerID,
		Cancel:        cancel,
		Observer:      observer,
		lastMessageID: "0",
	}

	a.subscriptionsMux.Lock()
	a.subscriptions[subscriptionID] = subscription
	a.subscriptionsMux.Unlock()

	go a.listenToStream(subCtx, subscription)

	a.logger.Info("Created subscription %s for stream: %s, group: %s", subscriptionID, stream, group)
	return subscriptionID, nil
}

// listenToStream is the main loop for a consumer to read from a stream.
func (a *ValkeyStreamsAdapter) listenToStream(ctx context.Context, sub *StreamSubscription) {
	sub.wg.Add(1)
	defer sub.wg.Done()

	jobs := make(chan redis.XMessage, a.jobQueueSize)
	var workerWg sync.WaitGroup
	for i := 0; i < a.workerPoolSize; i++ {
		workerWg.Add(1)
		go a.streamWorker(ctx, &workerWg, sub, jobs)
	}

	// Start a goroutine to claim pending messages periodically
	go a.claimPendingMessages(ctx, sub)

	for {
		select {
		case <-ctx.Done():
			close(jobs)
			workerWg.Wait()
			a.logger.Info("Stopping listener for stream: %s, group: %s", sub.Stream, sub.Group)
			return
		default:
			// Read new messages from the stream
			streams, err := a.client.XReadGroup(ctx, &redis.XReadGroupArgs{
				Group:    sub.Group,
				Consumer: sub.ConsumerID,
				Streams:  []string{sub.Stream, ">"}, // ">" means new messages only
				Count:    10,
				Block:    2 * time.Second,
			}).Result()

			if err != nil {
				if err != redis.Nil && ctx.Err() == nil {
					a.logger.Error("Error reading from stream %s: %v", sub.Stream, err)
					time.Sleep(1 * time.Second) // Avoid tight loop on error
				}
				continue
			}

			for _, stream := range streams {
				for _, msg := range stream.Messages {
					select {
					case jobs <- msg:
						// Message queued for processing
					case <-ctx.Done():
						return
					}
				}
			}
		}
	}
}

// streamWorker processes messages from the jobs channel.
func (a *ValkeyStreamsAdapter) streamWorker(ctx context.Context, wg *sync.WaitGroup, sub *StreamSubscription, jobs <-chan redis.XMessage) {
	defer wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-jobs:
			if !ok {
				return
			}
			// Process the message
			payload, ok := msg.Values["payload"].(string)
			if !ok {
				a.logger.Warn("Message %s has no 'payload' field, skipping", msg.ID)
				a.client.XAck(ctx, sub.Stream, sub.Group, msg.ID)
				continue
			}

			// Notify the observer
			select {
			case sub.Observer.Notify <- &ValkeyMessage{Topic: sub.Stream, Payload: payload}:
				// Acknowledge the message after successful processing
				if err := a.client.XAck(ctx, sub.Stream, sub.Group, msg.ID).Err(); err != nil {
					a.logger.Error("Failed to ACK message %s: %v", msg.ID, err)
				}
			case <-ctx.Done():
				return
			}
		}
	}
}

// claimPendingMessages periodically claims old pending messages from other consumers.
func (a *ValkeyStreamsAdapter) claimPendingMessages(ctx context.Context, sub *StreamSubscription) {
	ticker := time.NewTicker(a.claimInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			pending, err := a.client.XPendingExt(ctx, &redis.XPendingExtArgs{
				Stream: sub.Stream,
				Group:  sub.Group,
				Start:  "-",
				End:    "+",
				Count:  a.pendingMessageLimit,
			}).Result()

			if err != nil {
				a.logger.Error("Failed to check pending messages for stream %s: %v", sub.Stream, err)
				continue
			}

			for _, p := range pending {
				if p.Idle > a.claimInterval {
					a.logger.Info("Claiming stale message %s from consumer %s", p.ID, p.Consumer)
					claimArgs := &redis.XClaimArgs{
						Stream:   sub.Stream,
						Group:    sub.Group,
						Consumer: sub.ConsumerID,
						MinIdle:  a.claimInterval,
						Messages: []string{p.ID},
					}
					claimedMsgs, err := a.client.XClaim(ctx, claimArgs).Result()
					if err != nil {
						a.logger.Error("Failed to claim message %s: %v", p.ID, err)
						continue
					}
					// Process claimed messages immediately
					for _, msg := range claimedMsgs {
						// This could be sent to the worker pool as well
						payload, _ := msg.Values["payload"].(string)
						sub.Observer.Notify <- &ValkeyMessage{Topic: sub.Stream, Payload: payload}
						a.client.XAck(ctx, sub.Stream, sub.Group, msg.ID)
					}
				}
			}
		}
	}
}

// UnsubscribeFromStream stops listening to a stream and cleans up the subscription.
func (a *ValkeyStreamsAdapter) UnsubscribeFromStream(subscriptionID string) {
	a.subscriptionsMux.Lock()
	defer a.subscriptionsMux.Unlock()

	sub, exists := a.subscriptions[subscriptionID]
	if !exists {
		a.logger.Warn("Stream subscription ID not found: %s", subscriptionID)
		return
	}

	delete(a.subscriptions, subscriptionID)
	sub.Cancel()
	sub.wg.Wait()

	// Optional: Remove the consumer from the group.
	// Be cautious with this in a scaled environment.
	// a.client.XGroupDelConsumer(a.ctx, sub.Stream, sub.Group, sub.ConsumerID)

	a.logger.Info("Unsubscribed from stream: %s (ID: %s)", sub.Stream, subscriptionID)
}

func (a *ValkeyStreamsAdapter) PublishToStream(stream string, payload map[string]any) (string, error) {
	ctx, cancel := context.WithTimeout(a.ctx, 5*time.Second)
	defer cancel()

	// Serialize the entire payload map into a JSON string.
	jsonPayload, err := json.Marshal(payload)
	if err != nil {
		a.logger.Error("Failed to marshal payload to JSON for stream %s: %v", stream, err)
		return "", fmt.Errorf("failed to marshal payload: %w", err)
	}

	// The stream message will now have a single field 'data' containing the JSON string.
	streamMessage := map[string]any{
		"data": string(jsonPayload),
	}

	msgID, err := a.pubClient.XAdd(ctx, &redis.XAddArgs{
		Stream: stream,
		Values: streamMessage,
	}).Result()

	if err != nil {
		a.logger.Error("Failed to publish message to stream %s: %v", stream, err)
		return "", fmt.Errorf("failed to publish to stream: %w", err)
	}

	a.logger.Trace("Published message to stream %s with ID %s", stream, msgID)
	return msgID, nil
}

// Disconnect gracefully closes the connection and all subscriptions.
func (a *ValkeyStreamsAdapter) Disconnect() error {
	a.logger.Info("Disconnecting from Valkey (streams)...")
	a.cancel()

	a.subscriptionsMux.Lock()
	for _, sub := range a.subscriptions {
		sub.wg.Wait()
	}
	a.subscriptions = make(map[string]*StreamSubscription)
	a.subscriptionsMux.Unlock()

	var firstErr error
	if err := a.client.Close(); err != nil {
		a.logger.Error("Error closing Valkey streams client: %v", err)
		firstErr = err
	}
	if err := a.pubClient.Close(); err != nil {
		a.logger.Error("Error closing Valkey streams pub client: %v", err)
		if firstErr == nil {
			firstErr = err
		}
	}

	a.logger.Info("Disconnected from Valkey (streams)")
	return firstErr
}

// HealthCheck performs a health check on the Valkey connection.
func (a *ValkeyStreamsAdapter) HealthCheck() error {
	ctx, cancel := context.WithTimeout(a.ctx, 2*time.Second)
	defer cancel()
	return a.client.Ping(ctx).Err()
}

// GetStats returns connection and subscription statistics.
func (a *ValkeyStreamsAdapter) GetStats() map[string]any {
	a.subscriptionsMux.RLock()
	subscriptionCount := len(a.subscriptions)
	a.subscriptionsMux.RUnlock()

	stats := a.client.PoolStats()

	return map[string]any{
		"stream_subscriptions": subscriptionCount,
		"pool_hits":            stats.Hits,
		"pool_misses":          stats.Misses,
		"pool_timeouts":        stats.Timeouts,
		"pool_total_conns":     stats.TotalConns,
		"pool_idle_conns":      stats.IdleConns,
		"pool_stale_conns":     stats.StaleConns,
		"reconnect_attempts":   a.reconnectAttempts,
	}
}

func (r *ValkeyStreamsAdapter) Run(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			r.logger.Info("Stopping Valkey Streams adapter...")
			r.Disconnect()
			return
		case <-ticker.C:
			if err := r.HealthCheck(); err != nil {
				r.logger.Error("Valkey Streams health check failed: %v", err)
			} else {
				r.logger.Info("Valkey Streams is healthy")
			}

		case msg := <-r.publishQueue:
			if msg == nil {
				r.logger.Warn("Received nil message in publish queue, skipping")
				continue
			}
			if msg.Topic == "" || msg.Payload == nil {
				r.logger.Warn("Received message with empty topic or payload, skipping")
				continue
			}
			r.logger.Trace("Publishing message to stream %s", msg.Topic)

			payload, ok := msg.GetPayloadAsJSON()

			if !ok {
				r.logger.Warn("Failed to convert message payload to JSON for topic %s, skipping", msg.Topic)
				continue
			}

			if _, err := r.PublishToStream(msg.Topic, payload); err != nil {
				r.logger.Error("Failed to publish message to stream %s: %v", msg.Topic, err)
			} else {
				r.logger.Trace("Message published to stream %s successfully", msg.Topic)
			}

		}
	}
}

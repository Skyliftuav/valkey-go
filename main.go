package valkeygo

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/josh-tracey/scribe"
	"github.com/redis/go-redis/v9"

	"encoding/json"
	lru "github.com/hashicorp/golang-lru/v2" // IMPORTANT: Add this dependency
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

func GetEnvAsBool(key string, def bool) bool {
	if val, ok := os.LookupEnv(key); ok {
		switch val {
		case "true", "1", "TRUE", "True":
			return true
		case "false", "0", "FALSE", "False":
			return false
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

// --- ValkeyAdapter (Pub/Sub) ---

type ValkeyAdapter struct {
	client            *redis.Client
	pubClient         *redis.Client
	publishQueue      chan *PubMessage
	logger            *scribe.Logger
	subscriptions     map[string]*Subscription
	subscriptionsMux  sync.RWMutex
	ctx               context.Context
	cancel            context.CancelFunc
	reconnectAttempts int
	maxReconnectDelay time.Duration
	subWorkerPoolSize int
	subJobQueueSize   int
}

func NewValkeyAdapter(publishQueue chan *PubMessage, logger *scribe.Logger) *ValkeyAdapter {
	config := loadConfig()

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
		subWorkerPoolSize: GetEnvAsInt("VALKEY_SUB_WORKER_POOL_SIZE", 20),
		subJobQueueSize:   GetEnvAsInt("VALKEY_SUB_JOB_QUEUE_SIZE", 256),
	}
}

func (r *ValkeyAdapter) Connect() error {
	ctx, cancel := context.WithTimeout(r.ctx, 5*time.Second)
	defer cancel()

	if err := r.client.Ping(ctx).Err(); err != nil {
		r.logger.Error("Failed to connect to Valkey: %v", err)
		return fmt.Errorf("failed to connect to Valkey: %w", err)
	}

	if err := r.pubClient.Ping(ctx).Err(); err != nil {
		r.logger.Error("Failed to connect Valkey pub client: %v", err)
		return fmt.Errorf("failed to connect Valkey pub client: %w", err)
	}

	r.logger.Info("Connected to Valkey (Pub/Sub)")
	go r.monitorConnection()
	return nil
}

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

func (r *ValkeyAdapter) handleReconnection() {
	r.reconnectAttempts++
	delay := time.Duration(r.reconnectAttempts) * time.Second
	if delay > r.maxReconnectDelay {
		delay = r.maxReconnectDelay
	}

	r.logger.Info("Attempting to reconnect in %v (attempt %d)", delay, r.reconnectAttempts)
	time.Sleep(delay)

	if err := r.Connect(); err != nil {
		go r.handleReconnection()
	} else {
		r.logger.Info("Successfully reconnected")
		r.reconnectAttempts = 0
		r.resubscribeAll()
	}
}

func (r *ValkeyAdapter) Subscribe(topic string, observer Observer) string {
	subscriptionID := uuid.New().String()
	subCtx, cancel := context.WithCancel(r.ctx)

	sub := &Subscription{
		ID:       subscriptionID,
		Topic:    topic,
		Cancel:   cancel,
		Observer: observer,
	}

	r.subscriptionsMux.Lock()
	r.subscriptions[subscriptionID] = sub
	r.subscriptionsMux.Unlock()

	go r.startSubscription(subCtx, sub)
	return subscriptionID
}

func (r *ValkeyAdapter) startSubscription(ctx context.Context, sub *Subscription) {
	pubsub := r.client.PSubscribe(ctx, sub.Topic)
	defer pubsub.Close()

	if _, err := pubsub.Receive(ctx); err != nil {
		if ctx.Err() != context.Canceled {
			r.logger.Error("Failed to confirm subscription for %s: %v", sub.Topic, err)
		}
		return
	}

	jobs := make(chan *ValkeyMessage, r.subJobQueueSize)
	var wg sync.WaitGroup

	for i := 0; i < r.subWorkerPoolSize; i++ {
		wg.Add(1)
		go r.observerWorker(ctx, &wg, sub, jobs)
	}

	ch := pubsub.Channel()
	for {
		select {
		case <-ctx.Done():
			close(jobs)
			wg.Wait()
			return
		case msg, ok := <-ch:
			if !ok {
				close(jobs)
				wg.Wait()
				return
			}
			select {
			case jobs <- &ValkeyMessage{Topic: msg.Channel, Payload: msg.Payload}:
			default:
				r.logger.Warn("Job queue full for topic %s. Dropping message.", sub.Topic)
			}
		}
	}
}

func (r *ValkeyAdapter) observerWorker(ctx context.Context, wg *sync.WaitGroup, sub *Subscription, jobs <-chan *ValkeyMessage) {
	defer wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		case msg, ok := <-jobs:
			if !ok {
				return
			}
			select {
			case sub.Observer.Notify <- msg:
			case <-ctx.Done():
				return
			}
		}
	}
}

func (r *ValkeyAdapter) Unsubscribe(subID string) {
	r.subscriptionsMux.Lock()
	sub, exists := r.subscriptions[subID]
	delete(r.subscriptions, subID)
	r.subscriptionsMux.Unlock()

	if exists {
		sub.Cancel()
		r.logger.Info("Unsubscribed from %s", sub.Topic)
	}
}

// PublishBatch uses pipelines to reduce latency impact
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

	if _, err := pipe.Exec(ctx); err != nil {
		r.logger.Error("Batch publish failed: %v", err)
		return fmt.Errorf("batch publish failed: %w", err)
	}

	r.logger.Trace("Batch published %d messages", len(messages))
	return nil
}

func (r *ValkeyAdapter) Publish(topic string, message string) error {
	return r.PublishBatch([]*PubMessage{{Topic: topic, Payload: []byte(message)}})
}

func (r *ValkeyAdapter) Run(ctx context.Context) {
	batchSize := GetEnvAsInt("VALKEY_BATCH_SIZE", 50)
	batchTimeout := time.Duration(GetEnvAsInt("VALKEY_BATCH_TIMEOUT_MS", 50)) * time.Millisecond

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

		case msg := <-r.publishQueue:
			batch = append(batch, msg)
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

func (r *ValkeyAdapter) resubscribeAll() {
	r.subscriptionsMux.RLock()
	subs := make([]*Subscription, 0, len(r.subscriptions))
	for _, s := range r.subscriptions {
		subs = append(subs, s)
	}
	r.subscriptionsMux.RUnlock()

	for _, sub := range subs {
		newCtx, cancel := context.WithCancel(r.ctx)
		sub.Cancel = cancel
		go r.startSubscription(newCtx, sub)
	}
}

func (r *ValkeyAdapter) Disconnect() error {
	r.cancel()
	r.subscriptionsMux.Lock()
	r.subscriptions = make(map[string]*Subscription)
	r.subscriptionsMux.Unlock()
	return r.client.Close()
}

type StreamSubscription struct {
	ID         string
	Stream     string
	Group      string
	ConsumerID string
	Cancel     context.CancelFunc
	Observer   Observer
	wg         sync.WaitGroup
}

type ValkeyStreamsAdapter struct {
	client           *redis.Client
	pubClient        *redis.Client
	logger           *scribe.Logger
	publishQueue     chan *PubMessage
	subscriptions    map[string]*StreamSubscription
	subscriptionsMux sync.RWMutex

	// --- IDEMPOTENCY ---
	processedIDs *lru.Cache[string, bool]

	ctx    context.Context
	cancel context.CancelFunc

	reconnectAttempts   int
	maxReconnectDelay   time.Duration
	workerPoolSize      int
	jobQueueSize        int
	pendingMessageLimit int64
	claimInterval       time.Duration
}

func NewValkeyStreamsAdapter(publishQueue chan *PubMessage, logger *scribe.Logger) *ValkeyStreamsAdapter {
	config := loadConfig()

	// Initialize LRU Cache for deduplication (Last 1000 IDs)
	// This mirrors the Rust "processed_priority_ids" logic.
	cache, err := lru.New[string, bool](1000)
	if err != nil {
		logger.Error("Failed to create LRU cache: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	return &ValkeyStreamsAdapter{
		publishQueue:        publishQueue,
		logger:              logger,
		subscriptions:       make(map[string]*StreamSubscription),
		processedIDs:        cache,
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

func (a *ValkeyStreamsAdapter) Connect() error {
	ctx, cancel := context.WithTimeout(a.ctx, 5*time.Second)
	defer cancel()

	if err := a.client.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("streams connection failed: %w", err)
	}
	if err := a.pubClient.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("streams pub client failed: %w", err)
	}

	a.logger.Info("Connected to Valkey (Streams)")
	go a.monitorConnection()
	return nil
}

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

func (a *ValkeyStreamsAdapter) handleReconnection() {
	a.reconnectAttempts++
	delay := time.Duration(a.reconnectAttempts) * 2 * time.Second
	if delay > a.maxReconnectDelay {
		delay = a.maxReconnectDelay
	}

	a.logger.Info("Reconnect attempt (streams) %d in %v", a.reconnectAttempts, delay)
	time.Sleep(delay)

	if err := a.Connect(); err != nil {
		a.logger.Error("Reconnection failed (streams): %v", err)
	} else {
		a.logger.Info("Successfully reconnected (streams)")
		a.reconnectAttempts = 0
		a.resubscribeAll()
	}
}

func (a *ValkeyStreamsAdapter) SubscribeToStream(stream, group string, observer Observer) (string, error) {
	subscriptionID := uuid.New().String()
	consumerID := uuid.New().String()

	// Idempotent Group Creation
	err := a.client.XGroupCreateMkStream(a.ctx, stream, group, "0").Err()
	if err != nil && !isBusyGroupError(err) {
		return "", fmt.Errorf("failed to create group: %w", err)
	}

	subCtx, cancel := context.WithCancel(a.ctx)
	sub := &StreamSubscription{
		ID:         subscriptionID,
		Stream:     stream,
		Group:      group,
		ConsumerID: consumerID,
		Cancel:     cancel,
		Observer:   observer,
	}

	a.subscriptionsMux.Lock()
	a.subscriptions[subscriptionID] = sub
	a.subscriptionsMux.Unlock()

	go a.listenToStream(subCtx, sub)
	return subscriptionID, nil
}

func (a *ValkeyStreamsAdapter) listenToStream(ctx context.Context, sub *StreamSubscription) {
	sub.wg.Add(1)
	defer sub.wg.Done()

	jobs := make(chan redis.XMessage, a.jobQueueSize)
	var workerWg sync.WaitGroup

	for i := 0; i < a.workerPoolSize; i++ {
		workerWg.Add(1)
		go a.streamWorker(ctx, &workerWg, sub, jobs)
	}

	go a.claimPendingMessages(ctx, sub)

	for {
		select {
		case <-ctx.Done():
			close(jobs)
			workerWg.Wait()
			return
		default:
			// Block for 2 seconds waiting for new data
			streams, err := a.client.XReadGroup(ctx, &redis.XReadGroupArgs{
				Group:    sub.Group,
				Consumer: sub.ConsumerID,
				Streams:  []string{sub.Stream, ">"},
				Count:    10,
				Block:    2 * time.Second,
			}).Result()

			if err != nil {
				if err != redis.Nil && ctx.Err() == nil {
					a.logger.Error("Read error on stream %s: %v", sub.Stream, err)
					time.Sleep(1 * time.Second)
				}
				continue
			}

			for _, stream := range streams {
				for _, msg := range stream.Messages {
					select {
					case jobs <- msg:
					case <-ctx.Done():
						return
					}
				}
			}
		}
	}
}

// streamWorker - IMPORTS RUST IDEMPOTENCY LOGIC
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

			// Extract Payload
			payload, _ := msg.Values["data"].(string)
			if payload == "" {
				payload, _ = msg.Values["payload"].(string)
			}

			// --- DEDUPLICATION CHECK ---
			// Attempt to parse ID from CloudEvent JSON
			var event CloudEvent
			hasID := false
			if err := json.Unmarshal([]byte(payload), &event); err == nil && event.ID != "" {
				hasID = true
				if a.processedIDs.Contains(event.ID) {
					a.logger.Warn("Duplicate ID detected (bad network retry?): %s. Skipping logic, ACKing Redis.", event.ID)
					a.client.XAck(ctx, sub.Stream, sub.Group, msg.ID)
					continue
				}
			}

			// Notify Observer
			select {
			case sub.Observer.Notify <- &ValkeyMessage{Topic: sub.Stream, Payload: payload}:
				// Only mark processed IF successful and IF we had an ID
				if hasID {
					a.processedIDs.Add(event.ID, true)
				}
				// ACK the message to Redis so it doesn't re-send
				a.client.XAck(ctx, sub.Stream, sub.Group, msg.ID)
			case <-ctx.Done():
				return
			}
		}
	}
}

// PublishBatchToStream uses Pipelines for high throughput (Matches Rust perf)
func (a *ValkeyStreamsAdapter) PublishBatchToStream(batch []*PubMessage) error {
	if len(batch) == 0 {
		return nil
	}

	ctx, cancel := context.WithTimeout(a.ctx, 10*time.Second)
	defer cancel()

	pipe := a.pubClient.Pipeline()

	for _, msg := range batch {
		// Wrap in standard "data" key for compatibility
		values := map[string]any{"data": string(msg.Payload)}
		pipe.XAdd(ctx, &redis.XAddArgs{
			Stream: msg.Topic,
			Values: values,
		})
	}

	if _, err := pipe.Exec(ctx); err != nil {
		a.logger.Error("Stream batch publish failed: %v", err)
		return err
	}
	return nil
}

func (a *ValkeyStreamsAdapter) Run(ctx context.Context) {
	batchSize := 50
	batch := make([]*PubMessage, 0, batchSize)
	// Tick slightly slower for streams to allow accumulation
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			if len(batch) > 0 {
				a.PublishBatchToStream(batch)
			}
			a.Disconnect()
			return

		case msg := <-a.publishQueue:
			batch = append(batch, msg)
			if len(batch) >= batchSize {
				a.PublishBatchToStream(batch)
				batch = batch[:0]
			}

		case <-ticker.C:
			// Also run health check on tick
			a.HealthCheck()
			if len(batch) > 0 {
				a.PublishBatchToStream(batch)
				batch = batch[:0]
			}
		}
	}
}

// --- Housekeeping Functions ---

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
				continue
			}

			for _, p := range pending {
				if p.Idle > a.claimInterval {
					a.logger.Info("Claiming stale message %s", p.ID)
					msgs, err := a.client.XClaim(ctx, &redis.XClaimArgs{
						Stream:   sub.Stream,
						Group:    sub.Group,
						Consumer: sub.ConsumerID,
						MinIdle:  a.claimInterval,
						Messages: []string{p.ID},
					}).Result()

					if err == nil {
						// Note: We don't push to jobs channel here to avoid potential blocking
						// in this housekeeping routine. We could, but for now we let the
						// main loop pick it up or just Ack if it was a ghost.
						// Simplest for now: Just Log. If you need re-processing, push to observer.
						for _, m := range msgs {
							// Simple retry logic:
							val, _ := m.Values["data"].(string)
							sub.Observer.Notify <- &ValkeyMessage{Topic: sub.Stream, Payload: val}
							a.client.XAck(ctx, sub.Stream, sub.Group, m.ID)
						}
					}
				}
			}
		}
	}
}

func (a *ValkeyStreamsAdapter) UnsubscribeFromStream(subscriptionID string) {
	a.subscriptionsMux.Lock()
	sub, exists := a.subscriptions[subscriptionID]
	delete(a.subscriptions, subscriptionID)
	a.subscriptionsMux.Unlock()

	if exists {
		sub.Cancel()
		sub.wg.Wait()
		a.logger.Info("Unsubscribed from stream %s", sub.Stream)
	}
}

func (a *ValkeyStreamsAdapter) Disconnect() error {
	a.cancel()
	a.subscriptionsMux.Lock()
	for _, sub := range a.subscriptions {
		sub.wg.Wait()
	}
	a.subscriptions = make(map[string]*StreamSubscription)
	a.subscriptionsMux.Unlock()
	return a.client.Close()
}

func (a *ValkeyStreamsAdapter) HealthCheck() error {
	ctx, cancel := context.WithTimeout(a.ctx, 2*time.Second)
	defer cancel()
	return a.client.Ping(ctx).Err()
}

func (a *ValkeyStreamsAdapter) resubscribeAll() {
	a.subscriptionsMux.RLock()
	subs := make([]*StreamSubscription, 0, len(a.subscriptions))
	for _, sub := range a.subscriptions {
		subs = append(subs, sub)
	}
	a.subscriptionsMux.RUnlock()

	for _, sub := range subs {
		newCtx, cancel := context.WithCancel(a.ctx)
		sub.Cancel = cancel
		go a.listenToStream(newCtx, sub)
	}
}

// --- Common Helpers ---

type ValkeyConfig struct {
	Host         string
	Port         string
	Password     string
	Database     int
	PoolSize     int
	MinIdleConns int
}

func loadConfig() *ValkeyConfig {
	return &ValkeyConfig{
		Host:         GetEnv("VALKEY_HOST", "localhost"),
		Port:         GetEnv("VALKEY_PORT", "6379"),
		Password:     GetEnv("VALKEY_PASSWORD", ""),
		Database:     GetEnvAsInt("VALKEY_DATABASE", 0),
		PoolSize:     GetEnvAsInt("VALKEY_POOL_SIZE", 20),
		MinIdleConns: GetEnvAsInt("VALKEY_MIN_IDLE_CONNS", 10),
	}
}

func createValkeyClient(config *ValkeyConfig) *redis.Client {
	return redis.NewClient(&redis.Options{
		Addr:         fmt.Sprintf("%s:%s", config.Host, config.Port),
		Password:     config.Password,
		DB:           config.Database,
		PoolSize:     config.PoolSize,
		MinIdleConns: config.MinIdleConns,
	})
}

func isBusyGroupError(err error) bool {
	return err != nil && err.Error() == "BUSYGROUP Consumer Group name already exists"
}

func GetEnv(key, def string) string {
	if val, ok := os.LookupEnv(key); ok {
		return val
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

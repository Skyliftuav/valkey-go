# ValkeyGo

ValkeyGo is a high-level Go adapter for Valkey, built on top of the popular go-redis/v9 library. It provides a resilient, high-performance wrapper for Valkey's Pub/Sub and Streams capabilities.

It's designed to be integrated into services, providing a robust messaging layer with automatic reconnection, concurrent message processing, and simplified consumer group management.

The library offers two distinct adapters:

  - ValkeyAdapter: A client for robust Pub/Sub. It features per-subscription worker pools to prevent slow consumers from blocking message processing.

  - ValkeyStreamsAdapter: A client for Valkey Streams. It manages consumer groups, message acknowledgment (XACK), and automatic claiming of stale, pending messages to ensure reliable processing.

✨ Features

  - Built on go-redis/v9: Leverages the performance and stability of the standard Go client.

  - Dual Adapters: Provides separate, optimized clients for Pub/Sub and Streams.

  - Automatic Reconnection: Monitors the connection and automatically reconnects with exponential backoff.

  - Automatic Resubscription: Re-establishes all active subscriptions (both Pub/Sub and Streams) after a reconnection.

  - Concurrent Processing: Uses worker pools for both adapters to process messages concurrently, preventing head-of-line blocking from slow consumers.

  - Asynchronous Publishing:

      - ValkeyAdapter supports batched publishing for high throughput.

      - ValkeyStreamsAdapter uses a publish queue to handle XADD operations.

  - Reliable Stream Consumption: ValkeyStreamsAdapter automates consumer group creation, message acknowledgment, and claiming of stale messages from dead consumers.

  - Configuration via Environment: All settings are easily configurable using environment variables.

  - Observability: Includes HealthCheck() and GetStats() methods for monitoring.

📋 Requirements

  - Go 1.23.4+

  - Valkey server

💾 Installation

```bash

go get github.com/Skyliftuav/valkey-go
```

⚙️ Configuration

Both adapters are configured via environment variables, which are read automatically on creation.

🔗 Connection Settings (Common)

These settings apply to both ValkeyAdapter and ValkeyStreamsAdapter.
Environment Variable	Default	Description
```
VALKEY_HOST	localhost	Valkey server host.
VALKEY_PORT	6379	Valkey server port.
VALKEY_PASSWORD	""	Valkey password.
VALKEY_DATABASE	0	Valkey database index.
VALKEY_POOL_SIZE	10 (PubSub) / 20 (Streams)	Max number of socket connections.
VALKEY_MIN_IDLE_CONNS	5 (PubSub) / 10 (Streams)	Min number of idle connections.
VALKEY_MAX_RETRIES	3 (PubSub) / 5 (Streams)	Max retries on connection failure.
VALKEY_DIAL_TIMEOUT	5 (seconds)	Connection dial timeout.
VALKEY_READ_TIMEOUT	3 (seconds)	Read timeout.
VALKEY_WRITE_TIMEOUT	3 (seconds)	Write timeout.
```
📨 Pub/Sub Settings (ValkeyAdapter)

Environment Variable	Default	Description
```
VALKEY_SUB_WORKER_POOL_SIZE	20	Number of workers processing messages per subscription.
VALKEY_SUB_JOB_QUEUE_SIZE	256	Size of the job queue for each subscription.
VALKEY_BATCH_SIZE	10	Max messages in a single publish batch (for Run).
VALKEY_BATCH_TIMEOUT_MS	100	Max time (in ms) to wait before sending a batch (for Run).
```
🌊 Streams Settings (ValkeyStreamsAdapter)

Environment Variable	Default	Description
```
VALKEY_STREAM_WORKER_POOL_SIZE	10	Number of workers processing messages per stream.
VALKEY_STREAM_JOB_QUEUE_SIZE	256	Size of the job queue for stream workers.
VALKEY_STREAM_PENDING_LIMIT	100	Max pending messages to check for claiming.
VALKEY_STREAM_CLAIM_INTERVAL_SEC	60	Interval (in sec) to claim stale messages.
```
🚀 Usage

1. ValkeyAdapter (Pub/Sub)

This example shows how to connect, subscribe, and publish using the standard Pub/Sub adapter.
```Go

package main

import (
	"context"
	"log"
	"os"
	"time"

	"github.com/Skyliftuav/valkey-go" // <-- Import path
	"github.com/josh-tracey/scribe"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 1. Setup logger (uses scribe, but any logger works)
	logger := scribe.New(os.Stdout, scribe.LevelDebug)

	// 2. Setup publish queue (for async/batched publishing)
	publishQueue := make(chan *valkeygo.PubMessage, 100)

	// 3. Create and connect adapter
	adapter := valkeygo.NewValkeyAdapter(publishQueue, logger)
	if err := adapter.Connect(); err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer adapter.Disconnect()

	// 4. Start the async publish queue runner
	go adapter.Run(ctx)

	// 5. Create an observer to receive messages
	myObserver := valkeygo.Observer{
		Notify: make(chan *valkeygo.ValkeyMessage),
	}

	// 6. Subscribe to a topic
	subID := adapter.Subscribe("my-topic", myObserver)
	log.Printf("Subscribed with ID: %s", subID)

	// 7. Start a goroutine to listen for messages
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case msg := <-myObserver.Notify:
				payload, _ := msg.GetPayloadAsString()
				log.Printf("Received message on topic %s: %s", msg.GetDestinationName(), payload)
			}
		}
	}()

	// 8. Publish messages
	
	// Example 1: Synchronous publish (blocks until sent)
	if err := adapter.Publish("my-topic", "Hello Valkey! (sync)"); err != nil {
		log.Printf("Failed to publish sync message: %v", err)
	}

	// Example 2: Asynchronous/batched publish via the queue
	publishQueue <- &valkeygo.PubMessage{
		Topic:   "my-topic",
		Payload: []byte("Hello Valkey! (async)"),
	}

	// 9. Wait a bit, then unsubscribe and shut down
	time.Sleep(2 * time.Second)
	adapter.Unsubscribe(subID)
	log.Println("Shutting down...")
}
```

2. ValkeyStreamsAdapter (Streams)

This example shows how to use consumer groups to reliably process messages from a Valkey Stream.
```Go

package main

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"time"

	"github.com/Skyliftuav/valkey-go" // <-- Import path
	"github.com/josh-tracey/scribe"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// 1. Setup logger
	logger := scribe.New(os.Stdout, scribe.LevelDebug)

	// 2. Setup publish queue
	publishQueue := make(chan *valkeygo.PubMessage, 100)

	// 3. Create and connect streams adapter
	adapter := valkeygo.NewValkeyStreamsAdapter(publishQueue, logger)
	if err := adapter.Connect(); err != nil {
		log.Fatalf("Failed to connect: %v", err)
	}
	defer adapter.Disconnect()

	// 4. Start the async publish queue runner
	// This runner handles XADD commands for streams.
	go adapter.Run(ctx)

	// 5. Create an observer
	myObserver := valkeygo.Observer{
		Notify: make(chan *valkeygo.ValkeyMessage),
	}

	// 6. Subscribe to a stream
	// This creates the stream and consumer group if they don't exist.
	subID, err := adapter.SubscribeToStream("my-stream", "my-consumer-group", myObserver)
	if err != nil {
		log.Fatalf("Failed to subscribe: %v", err)
	}
	log.Printf("Subscribed to stream with ID: %s", subID)

	// 7. Start a goroutine to listen for messages
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			case msg := <-myObserver.Notify:
				// Messages are ACK'd automatically by the worker
				log.Printf("Received stream message (stream %s): %s", msg.GetDestinationName(), msg.Payload)
			}
		}
	}()

	// 8. Publish messages to the stream via the queue
	// The adapter's Run() method expects the payload to be JSON-marshallable.
	payload := map[string]any{
		"message": "Hello Stream!",
		"id":      123,
	}
	payloadBytes, _ := json.Marshal(payload)

	publishQueue <- &valkeygo.PubMessage{
		Topic:   "my-stream", // Topic is used as the Stream name
		Payload: payloadBytes,
	}

	// 9. Wait a bit, then shut down
	time.Sleep(3 * time.Second) // Give time for XReadGroup to block and receive
	adapter.UnsubscribeFromStream(subID)
	log.Println("Shutting down...")
}
```

3. Helper: Creating CloudEvents

The library includes a helper to easily create CloudEvent v1.0 messages, which can then be published.
```Go

import (
    "encoding/json"
    "time"
    "github.com/Skyliftuav/valkey-go"
)

// ...

// 1. Define your event data
data := map[string]string{"foo": "bar"}

// 2. Create the CloudEvent
event := valkeygo.NewCloudEvent(
    "my-service",        // source
    "com.example.myevent", // type
    "my-subject",        // subject
    time.Now().UTC().Format(time.RFC3339), // time
    data,                // data
)

// 3. Marshal to JSON
eventBytes, _ := json.Marshal(event)

// 4. Publish it (e.g., to the Pub/Sub async queue)
publishQueue <- &valkeygo.PubMessage{
    Topic:   "my-topic-or-stream",
    Payload: eventBytes,
}
```

⚖️ License

This library is licensed under the MIT License. See the LICENSE file for details.

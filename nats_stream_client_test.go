package nsclient

import (
	"context"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	natsserver "github.com/nats-io/nats-server/v2/server"
	natsclient "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// const TEST_PORT = 4222

// func RunServerOnPort(port int) *server.Server {
// 	opts := natsserver.DefaultTestOptions
// 	opts.Port = port
// 	return RunServerWithOptions(&opts)
// }

// func RunServerWithOptions(opts *server.Options) *server.Server {
// 	return natsserver.RunServer(opts)
//}

func NewInProcessNATSServer() (conn *natsclient.Conn, js jetstream.JetStream, cleanup func(), err error) {
	tmp, err := os.MkdirTemp("", "nats_test")
	if err != nil {
		err = fmt.Errorf("failed to create temp directory for NATS storage: %w", err)
		return
	}
	server, err := natsserver.NewServer(&natsserver.Options{
		DontListen: true, // Don't make a TCP socket.
		JetStream:  true,
		StoreDir:   tmp,
	})
	if err != nil {
		err = fmt.Errorf("failed to create NATS server: %w", err)
		return
	}
	// Add logs to stdout.
	// server.ConfigureLogger()
	server.Start()
	cleanup = func() {
		server.Shutdown()
		os.RemoveAll(tmp)
	}

	if !server.ReadyForConnections(time.Second * 5) {
		err = errors.New("failed to start server after 5 seconds")
		return
	}

	// Create a connection.
	conn, err = natsclient.Connect("", natsclient.InProcessServer(server))
	if err != nil {
		err = fmt.Errorf("failed to connect to server: %w", err)
		return
	}

	// Create a JetStream client.
	js, err = jetstream.New(conn)
	if err != nil {
		err = fmt.Errorf("failed to create jetstream: %w", err)
		return
	}

	return
}
func TestNatsStreamClient(t *testing.T) {
	// Arrange.
	conn, js, shutdown, err := NewInProcessNATSServer()
	if err != nil {
		t.Fatal(err)
	}
	defer shutdown()
	ctx := context.Background()

	streamName := "test-stream-2"
	// // Create a stream.
	// streamName := "test_stream"
	// _, err = js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
	// 	Name:     streamName,
	// 	Subjects: []string{"*"},
	// 	Storage:  jetstream.MemoryStorage, // For speed in tests.
	// })
	// if err != nil {
	// 	t.Fatalf("failed to create stream: %v", err)
	// }

	//Create a durable consumer.
	// consumer, err := js.CreateOrUpdateConsumer(ctx, streamName, jetstream.ConsumerConfig{
	// 	//Durable:       "testBatchProcessor",
	// 	MemoryStorage: true, // For speed in tests.
	// })
	// if err != nil {
	// 	t.Fatalf("unexpected failure creating or updating consumer: %v", err)
	// }

	t.Run("should publish a message to the stream and subject with the stream and subject being created automatically", func(t *testing.T) {
		subject := "test-subject"
		payload := "test-payload"
		nc, err := NewNatsStreamClient(conn, js, streamName)
		if err != nil {
			t.Fatalf("failed to create stream: %v", err)
		}
		err = nc.Publish(ctx, subject, []byte(payload))
		if err != nil {
			t.Fatalf("failed to publish to stream: %v", err)
		}

		filter := fmt.Sprintf("%s.%s", streamName, subject)
		c1, err := js.CreateOrUpdateConsumer(ctx, streamName, jetstream.ConsumerConfig{
			Name: "consumer-a",
			//Durable:       "testBatchProcessor",
			MemoryStorage: true, // For speed in tests.
			FilterSubject: filter,
		})
		if err != nil {
			t.Fatalf("unexpected failure creating or updating consumer: %v", err)
		}

		batch, err := c1.Fetch(1)
		if err != nil {
			t.Fatalf("unexpected failure fetching messages: %v", err)
		}

		for m := range batch.Messages() {
			m.Ack()
			if string(m.Data()) != payload {
				t.Fatalf("unexpected payload: %s expected %s", string(m.Data()), payload)
			}
		}

	})

	t.Run("should publish 2 messages to the stream with different subjects with the stream and subjects being created automatically", func(t *testing.T) {
		subject1 := "test-subject1"
		subject2 := "test-subject2"
		payload1 := "test-payload1"
		payload2 := "test-payload2"
		nc, err := NewNatsStreamClient(conn, js, streamName)
		if err != nil {
			t.Fatalf("failed to create stream: %v", err)
		}
		err = nc.Publish(ctx, subject1, []byte(payload1))
		if err != nil {
			t.Fatalf("failed to publish to stream: %v", err)
		}

		err = nc.Publish(ctx, subject2, []byte(payload2))
		if err != nil {
			t.Fatalf("failed to publish to stream: %v", err)
		}

		c1, err := js.CreateOrUpdateConsumer(ctx, streamName, jetstream.ConsumerConfig{
			Name: "consumer-a",
			//Durable:       "testBatchProcessor",
			MemoryStorage: true, // For speed in tests.
			FilterSubject: fmt.Sprintf("%s.%s", streamName, subject1),
		})
		if err != nil {
			t.Fatalf("unexpected failure creating or updating consumer: %v", err)
		}

		c2, err := js.CreateOrUpdateConsumer(ctx, streamName, jetstream.ConsumerConfig{
			Name: "consumer-b",
			//Durable:       "testBatchProcessor",
			MemoryStorage: true, // For speed in tests.
			FilterSubject: fmt.Sprintf("%s.%s", streamName, subject2),
		})
		if err != nil {
			t.Fatalf("unexpected failure creating or updating consumer: %v", err)
		}

		batch1, err := c1.Fetch(1)
		if err != nil {
			t.Fatalf("unexpected failure fetching messages: %v", err)
		}

		for m := range batch1.Messages() {
			m.Ack()
			if string(m.Data()) != payload1 {
				t.Fatalf("unexpected payload: %s expected %s", string(m.Data()), payload1)
			}
		}

		batch2, err := c2.Fetch(1)
		if err != nil {
			t.Fatalf("unexpected failure fetching messages: %v", err)
		}

		for m := range batch2.Messages() {
			m.Ack()
			if string(m.Data()) != payload2 {
				t.Fatalf("unexpected payload: %s expected %s", string(m.Data()), payload2)
			}
		}

	})

	t.Run("should publish a message to the stream and subject and get the message back using client method", func(t *testing.T) {
		subject := "test-subject1"
		payload := "test-payload1"
		nc, err := NewNatsStreamClient(conn, js, streamName)
		if err != nil {
			t.Fatalf("failed to create stream: %v", err)
		}
		err = nc.Publish(ctx, subject, []byte(payload))
		if err != nil {
			t.Fatalf("failed to publish to stream: %v", err)
		}

		nc1, err := NewNatsStreamClient(conn, js, streamName)
		if err != nil {
			t.Fatalf("failed to create stream: %v", err)
		}
		msg, err := nc1.Get(ctx, subject, 1, "consumer-a")
		if err != nil {
			t.Fatalf("failed to get from stream: %v", err)
		}

		if string(msg[0]) != payload {
			t.Fatalf("unexpected payload: %s expected %s", string(msg[0]), payload)
		}

	})

	// t.Run("if no messages are present in the stream, the processor function is not called", func(t *testing.T) {
	// 	p := func(ctx context.Context, msgs []BatchMessage) []error {
	// 		t.Errorf("unexpected call to process function")
	// 		return make([]error, len(msgs))
	// 	}
	// 	bp := NewBatchProcessor[BatchMessage](consumer, 10, p, jetstream.FetchMaxWait(time.Millisecond))
	// 	if err := bp.Process(ctx); err != nil {
	// 		t.Fatalf("unexpected error processing batch: %v", err)
	// 	}
	// })
}

func TestNatsClient_Publish(t *testing.T) {
	// Create a new NatsClient instance

	// natsClient,e := NewNatsStreamClient("nats://1.1.1.1:4222", "test-stream")
	// if e != nil {
	// 	t.Errorf("NewNatsStreamClient returned an error: %v", e)
	// }
	// // Create a mock context
	// ctx := context.TODO()

	// // Define test subject and payload
	// subject := "test.subject"
	// payload := []byte("test payload")

	// // Mock the Publish method of the underlying nats.JetStream instance
	// mockPublish := func(ctx context.Context, subject string, payload []byte) {
	// 	// Do nothing for the mock
	// }

	// // Set the mockPublish function as the Publish method of nats.JetStream
	// natsClient.Js.Publish = mockPublish

	// // Call the Publish method
	// err := natsClient.Publish(ctx, subject, payload)

	// // Check if there was an error
	// if err != nil {
	// 	t.Errorf("Publish returned an error: %v", err)
	// }

	// TODO: Add additional assertions for the expected behavior of the Publish method
}

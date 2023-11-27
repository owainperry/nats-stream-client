package nsclient

import (
	"context"
	"fmt"
	//"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	log "github.com/sirupsen/logrus"
)

type NatsStreamClient struct {
	NatsUri    string
	Connection *nats.Conn
	JS         jetstream.JetStream
	streamName string
}

//	Create a NatStreamClient without access to the connection or jetstream	
func NatsStreamClientFactory(natsUri string, streamName string) (NatsStreamClient, error) {
	var err error
	var rtn NatsStreamClient

	nc, err := nats.Connect(natsUri)
	if err != nil {
		return rtn, err
	}
	defer nc.Close()
	defer nc.Drain()

	js, err := jetstream.New(nc)
	if err != nil {
		log.Fatal(err)
	}

	rtn, err = NewNatsStreamClient(nc, js, streamName)
	return rtn, err
}

func NewNatsStreamClient(conn *nats.Conn, js jetstream.JetStream, streamName string) (NatsStreamClient, error) {
	var err error
	var rtn NatsStreamClient

	rtn = NatsStreamClient{
		Connection: conn,
		JS:         js,
		streamName: streamName,
	}

	return rtn, err
}

func (n *NatsStreamClient) streamExists(ctx context.Context, js jetstream.JetStream, streamName string) bool {

	list := js.ListStreams(ctx)
	for v := range list.Info() {
		if v.Config.Name == streamName {
			return true
		}
	}

	return false
}

func (n *NatsStreamClient) appendSubjectToStream(ctx context.Context, js jetstream.JetStream, streamName string, subject string) error {
	stream, err := js.Stream(ctx, streamName)
	if err != nil {
		return err
	}
	info, err := stream.Info(ctx)
	if err != nil {
		return err
	}
	config := info.Config

	exsits := n.subjectExists(config, subject)
	log.Infof("subject %s exists: %t", subject, exsits)
	newSubjects := append(config.Subjects, subject)
	if !exsits {
		cfg := jetstream.StreamConfig{
			Name:      streamName,
			Retention: jetstream.WorkQueuePolicy,
			Subjects:  newSubjects,
		}

		log.Infof("update the stream %s subjects with %s", streamName, subject)
		_, err = js.CreateOrUpdateStream(ctx, cfg)
		if err != nil {
			log.Infof("%s", err)
			return err
		}
		log.Info("updated stream in theory")
	}
	return nil
}

func (n *NatsStreamClient) subjectExists(config jetstream.StreamConfig, subject string) bool {
	found := false
	for _, v := range config.Subjects {
		if v == subject {
			found = true
		}
	}
	return found
}

func (n *NatsStreamClient) convertSubjectsToUnique(subjects []string) []string {
	rtn := []string{}
	for _, v := range subjects {
		rtn = append(rtn, fmt.Sprintf("%s.%s", n.streamName, v))
	}
	return rtn
}

func (n *NatsStreamClient) createStream(ctx context.Context, subjects []string) error {

	if !n.streamExists(ctx, n.JS, n.streamName) {
		cfg := jetstream.StreamConfig{
			Name:      n.streamName,
			Retention: jetstream.WorkQueuePolicy,
			Subjects:  subjects,
		}

		log.Infof("create the stream %s", n.streamName)
		_, err := n.JS.CreateOrUpdateStream(ctx, cfg)
		if err != nil {
			log.Error(err)
			return err
		}
		log.Info("created the stream")
	} else {
		for _, v := range subjects {
			n.appendSubjectToStream(ctx, n.JS, n.streamName, v)
		}
	}
	return nil
}

// func (n *NatsStreamClient) fullSubjectName(subject string) string {
// 	return fmt.Sprintf("%s.%s", n.streamName, subject)
// }

func (n *NatsStreamClient) Publish(ctx context.Context, subject string, payload []byte) error {

	//fullSubject := n.fullSubjectName(subject)

	uniqueSubjects := n.convertSubjectsToUnique([]string{subject})

	err := n.createStream(ctx, uniqueSubjects)
	if err != nil {
		log.Error(err)
		return err
	}
	log.Infof("Published to [%s]: []'%s']", uniqueSubjects[0], payload)
	n.JS.Publish(ctx, uniqueSubjects[0], payload)

	if err := n.Connection.LastError(); err != nil {
		return err
	}
	log.Infof("Published to [%s]: []'%s']", subject, payload)
	return nil
}

func (n *NatsStreamClient) Get(ctx context.Context, subject string, numberOfMessages int, consumerName string) ([][]byte, error) {
	log.Infof("get %d messages from %s consumername %s", numberOfMessages, subject, consumerName)

	rtn := [][]byte{}

	filter := n.convertSubjectsToUnique([]string{subject})[0]
	log.Infof("filter: %s", filter)
	c1, err := n.JS.CreateOrUpdateConsumer(ctx, n.streamName, jetstream.ConsumerConfig{
		Name:          consumerName,
		FilterSubject: filter,
	})
	if err != nil {
		log.Error("failed to create consumer")
		return rtn, err
	}

	batch, err := c1.Fetch(numberOfMessages)
	if err != nil {
		log.Error("failed to fetch messages")
		return rtn, err
	}
	log.Infof("got %d messages", len(batch.Messages()))
	for m := range batch.Messages() {
		err := m.Ack()
		if err != nil {
			log.Error("failed to ack message")
			return rtn, err
		}
		rtn = append(rtn, m.Data())
	}

	return rtn, nil
}

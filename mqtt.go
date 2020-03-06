package mqttpubsub

import (
	"bytes"
	"context"
	"encoding/gob"
	"errors"
	"github.com/google/uuid"
	"gocloud.dev/gcerrors"
	"gocloud.dev/pubsub/driver"
	"strings"
	"sync"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

const (
	defaultTimeout = 2 * time.Second
	defaultQOS     = 0
	defaultPubID   = "go-cloud-publisher"
	defaultSubID   = "go-cloud-subscriber"
	delimiter      = "-"
)

var (
	errNoURLEnv         = errors.New("mqttpubsub: no url env provided ")
	errInvalidMessage   = errors.New("mqttpubsub: invalid or empty message")
	errConnRequired     = errors.New("mqttpubsub: mqtt connection is required")
	errStillConnected   = errors.New("mqttpubsub: still connected. Kill all processes manually")
	errMQTTDisconnected = errors.New("mqttpubsub: disconnected")
)

type (
	Subscriber interface {
		Subscribe(topic string, handler mqtt.MessageHandler, qos *byte) error
		UnSubscribe(topic string) error
		Close() error
	}

	Publisher interface {
		Publish(topic string, payload interface{}, qos *byte) error
		Stop() error
	}

	subscriber struct {
		subConnect mqtt.Client

		qos     byte
		timeout time.Duration
	}

	publisher struct {
		pubConnect mqtt.Client

		qos     byte
		timeout time.Duration

		isStopped bool
		wg        *sync.WaitGroup
	}
)

func NewSubscriber(cli mqtt.Client, qos byte, timeout time.Duration) Subscriber {
	return &subscriber{
		subConnect: cli,
		qos:        qos,
		timeout:    timeout,
	}
}

func NewPublisher(cli mqtt.Client, qos byte, timeout time.Duration) Publisher {
	return &publisher{
		pubConnect: cli,
		qos:        qos,
		timeout:    timeout,
		wg:         new(sync.WaitGroup),
	}
}

func defaultSubClient(url string) (_ Subscriber, err error) {
	if url == "" {
		return nil, errNoURLEnv
	}

	var subConnect mqtt.Client
	subConnect, err = makeDefaultConnect(defaultSubID, url)
	if err != nil {
		return nil, err
	}
	return &subscriber{
		subConnect: subConnect,
		timeout:    defaultTimeout,
		qos:        defaultQOS,
	}, nil
}

func defaultPubClient(url string) (_ Publisher, err error) {
	if url == "" {
		return nil, errNoURLEnv
	}

	var pubConnect mqtt.Client
	pubConnect, err = makeDefaultConnect(defaultPubID, url)
	if err != nil {
		return nil, err
	}
	return &publisher{
		pubConnect: pubConnect,
		wg:         new(sync.WaitGroup),
		timeout:    defaultTimeout,
		qos:        defaultQOS,
	}, nil
}

func makeDefaultConnect(clientID, url string) (mqtt.Client, error) {
	connID := uuid.New().String()
	opts := mqtt.NewClientOptions()
	opts = opts.AddBroker(url)
	opts.ClientID = clientID + delimiter + connID
	mqttClient := mqtt.NewClient(opts)
	token := mqttClient.Connect()
	token.Wait()
	if token.Error() != nil {
		return nil, token.Error()
	}
	if !mqttClient.IsConnectionOpen() {
		return nil, errMQTTDisconnected
	}

	return mqttClient, nil
}

// qos is optional
func (p *publisher) Publish(topic string, payload interface{}, qos *byte) error {
	if p.isStopped {
		return nil
	}

	var q = p.qos
	if qos != nil {
		q = *qos
	}

	token := p.pubConnect.Publish(topic, q, false, payload)
	if token.WaitTimeout(p.timeout) && token.Error() != nil {
		return token.Error()
	}
	return nil
}

func (p *publisher) Stop() error {
	if p.pubConnect == nil {
		return nil
	}
	if p.isStopped {
		return nil
	}
	p.isStopped = true
	p.wg.Wait()
	p.pubConnect.Disconnect(0)
	if p.pubConnect.IsConnected() {
		return errStillConnected
	}
	return nil
}

// qos is optional
func (s *subscriber) Subscribe(topic string, handler mqtt.MessageHandler, qos *byte) error {
	if !s.subConnect.IsConnected() {
		return errMQTTDisconnected
	}

	topic = strings.TrimSuffix(strings.TrimPrefix(topic, "/"), "/")

	var q = s.qos
	if qos != nil {
		q = *qos
	}

	token := s.subConnect.Subscribe(
		topic,
		q,
		handler,
	)

	if token.WaitTimeout(s.timeout) && token.Error() != nil {
		return token.Error()
	}

	return nil
}

func (s *subscriber) UnSubscribe(topic string) error {
	if !s.subConnect.IsConnected() {
		return errMQTTDisconnected
	}

	token := s.subConnect.Unsubscribe(topic)
	if token.WaitTimeout(s.timeout) && token.Error() != nil {
		return token.Error()
	}
	return nil
}

func (s *subscriber) Close() error {
	if s.subConnect == nil {
		return nil
	}
	if !s.subConnect.IsConnected() {
		return nil
	}
	s.subConnect.Disconnect(0)
	if s.subConnect.IsConnected() {
		return errStillConnected
	}
	return nil
}

// Convert MQTT msgs to *driver.Message.
func decode(msg mqtt.Message) (*driver.Message, error) {
	if msg == nil {
		return nil, errInvalidMessage
	}

	var dm driver.Message
	if err := decodeMessage(msg.Payload(), &dm); err != nil {
		return nil, err
	}
	dm.AckID = msg.MessageID() // uint16
	dm.AsFunc = messageAsFunc(msg)
	return &dm, nil
}

func messageAsFunc(msg mqtt.Message) func(interface{}) bool {
	return func(i interface{}) bool {
		p, ok := i.(*mqtt.Message)
		if !ok {
			return false
		}
		*p = msg
		return true
	}
}

func encodeMessage(dm *driver.Message) ([]byte, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if len(dm.Metadata) == 0 {
		return dm.Body, nil
	}
	if err := enc.Encode(dm.Metadata); err != nil {
		return nil, err
	}

	if err := enc.Encode(dm.Body); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func decodeMessage(data []byte, dm *driver.Message) error {
	buf := bytes.NewBuffer(data)
	dec := gob.NewDecoder(buf)
	if err := dec.Decode(&dm.Metadata); err != nil {
		dm.Metadata = nil
		dm.Body = data
		return nil
	}
	return dec.Decode(&dm.Body)
}

func whichError(err error) gcerrors.ErrorCode {
	switch err {
	case nil:
		return gcerrors.OK
	case context.Canceled:
		return gcerrors.Canceled
	case errMQTTDisconnected, errConnRequired:
		return gcerrors.NotFound
	case mqtt.ErrInvalidTopicEmptyString, mqtt.ErrInvalidQos, mqtt.ErrInvalidTopicMultilevel, errInvalidMessage:
		return gcerrors.FailedPrecondition
	case errStillConnected:
		return gcerrors.Internal
	}
	return gcerrors.Unknown
}

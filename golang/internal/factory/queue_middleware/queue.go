package queue_middleware

import (
	"context"
	"fmt"
	"time"

	m "github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/middleware"
	amqp "github.com/rabbitmq/amqp091-go"
)

type QueueMiddleWare struct {
	Conn        *amqp.Connection
	Channel     *amqp.Channel
	Queue       amqp.Queue
	consumerTag string
}

func (q *QueueMiddleWare) StartConsuming(callbackFunc func(msg m.Message, ack func(), nack func())) error {
	if q.consumerTag != "" {
		return nil
	}

	// unique tag for each consumer
	tag := fmt.Sprintf("consumer-%s-%d", q.Queue.Name, time.Now().UnixNano())

	msgs, err := q.Channel.Consume(
		q.Queue.Name,
		tag,
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return q.wrapChannelError(err)
	}

	q.consumerTag = tag

	go func() {
		for d := range msgs {
			msg := m.Message{Body: string(d.Body)}
			ack := func() {
				d.Ack(false)
			}
			nack := func() {
				d.Nack(false, true)
			}
			callbackFunc(msg, ack, nack)
		}
	}()

	return nil
}

func (q *QueueMiddleWare) StopConsuming() error {
	if q.consumerTag == "" {
		return nil
	}

	err := q.Channel.Cancel(q.consumerTag, false)
	if err != nil {
		return q.wrapChannelError(err)
	}

	q.consumerTag = ""
	return nil
}

func (q *QueueMiddleWare) Send(msg m.Message) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	body := msg.Body
	err := q.Channel.PublishWithContext(
		ctx,
		"",
		q.Queue.Name,
		false,
		false,
		amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			ContentType:  "text/plain",
			Body:         []byte(body),
		})
	if err != nil {
		return q.wrapChannelError(err)
	}

	return nil
}

func (q *QueueMiddleWare) Close() error {
	if err := q.Channel.Close(); err != nil {
		return m.ErrMessageMiddlewareClose
	}

	if err := q.Conn.Close(); err != nil {
		return m.ErrMessageMiddlewareClose
	}

	return nil
}

func (q *QueueMiddleWare) wrapChannelError(err error) error {
	if err == nil {
		return nil
	}

	if err == amqp.ErrClosed || (q.Channel != nil && q.Channel.IsClosed()) {
		q.consumerTag = ""
		return m.ErrMessageMiddlewareDisconnected
	}

	return m.ErrMessageMiddlewareMessage
}

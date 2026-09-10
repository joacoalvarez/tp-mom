package queue_middleware

import (
	"context"
	"time"

	baseM "github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/factory/base_middleware"
	m "github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/middleware"
	amqp "github.com/rabbitmq/amqp091-go"
)

type QueueMiddleware struct {
	baseM.BaseMiddleware
	Queue amqp.Queue
}

func (q *QueueMiddleware) StartConsuming(callbackFunc func(msg m.Message, ack func(), nack func())) error {
	if q.ConsumerTag != "" {
		return nil
	}

	// unique tag for each consumer
	tag := q.GetConsumerTag(q.Queue.Name)

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
		return q.WrapChannelError(err)
	}

	q.ConsumerTag = tag

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

func (q *QueueMiddleware) Send(msg m.Message) error {
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
		return q.WrapChannelError(err)
	}

	return nil
}

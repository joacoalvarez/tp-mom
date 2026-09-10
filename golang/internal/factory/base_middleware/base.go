package basemiddlware

import (
	"fmt"
	"time"

	m "github.com/7574-sistemas-distribuidos/tp-mom/golang/internal/middleware"
	amqp "github.com/rabbitmq/amqp091-go"
)

type BaseMiddleware struct {
	Conn        *amqp.Connection
	Channel     *amqp.Channel
	ConsumerTag string
}

func (b *BaseMiddleware) GetConsumerTag(name string) string {
	if b.ConsumerTag != "" {
		return b.ConsumerTag
	}
	return fmt.Sprintf("consumer-%s-%d", name, time.Now().UnixNano())
}

func (b *BaseMiddleware) WrapChannelError(err error) error {
	if err == nil {
		return nil
	}

	if err == amqp.ErrClosed || (b.Channel != nil && b.Channel.IsClosed()) {
		b.ConsumerTag = ""
		return m.ErrMessageMiddlewareDisconnected
	}

	return m.ErrMessageMiddlewareMessage
}

func (b *BaseMiddleware) StopConsuming() error {
	if b.ConsumerTag == "" {
		return nil
	}

	err := b.Channel.Cancel(b.ConsumerTag, false)
	if err != nil {
		return b.WrapChannelError(err)
	}

	b.ConsumerTag = ""
	return nil
}

func (b *BaseMiddleware) Close() error {
	if err := b.Channel.Close(); err != nil {
		return m.ErrMessageMiddlewareClose
	}

	if err := b.Conn.Close(); err != nil {
		return m.ErrMessageMiddlewareClose
	}

	return nil
}

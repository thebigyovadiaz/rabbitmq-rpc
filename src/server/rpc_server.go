package main

import (
	"context"
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/thebigyovadiaz/rabbitmq-rpc/src/util"
	"strconv"
	"time"
)

func fib(n int) int {
	if n == 0 {
		return 0
	} else if n == 1 {
		return 1
	} else {
		return fib(n-1) + fib(n-2)
	}
}

func main() {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672")
	util.LogFailOnError(err, "Failed to connect to RabbitMQ")
	util.LogSuccessful("Connected to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	util.LogFailOnError(err, "Failed to open a channel")
	util.LogSuccessful("Channel open successfully")
	defer ch.Close()

	qD, err := ch.QueueDeclare(
		"rpc_queue",
		false,
		false,
		false,
		false,
		nil,
	)
	util.LogFailOnError(err, "Failed to declare a queue")

	err = ch.Qos(
		1,
		0,
		false,
	)
	util.LogFailOnError(err, "Failed to set QoS")

	messages, err := ch.Consume(
		qD.Name,
		"",
		false,
		false,
		false,
		false,
		nil,
	)
	util.LogFailOnError(err, "Failed to register a consumer")

	var forever chan struct{}

	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		for d := range messages {
			n, err := strconv.Atoi(string(d.Body))
			util.LogFailOnError(err, "Failed to convert body to integer")

			util.LogSuccessful(fmt.Sprintf(" [.] fib(%d)", n))
			response := fib(n)

			err = ch.PublishWithContext(
				ctx,
				"",
				d.ReplyTo,
				false,
				false,
				amqp.Publishing{
					ContentType:   "text/plain",
					CorrelationId: d.CorrelationId,
					Body:          []byte(strconv.Itoa(response)),
				},
			)

			util.LogFailOnError(err, "Failed to publish a message")
			d.Ack(false)
		}
	}()

	util.LogSuccessful(" [*] Awaiting RPC Requests")
	<-forever
}

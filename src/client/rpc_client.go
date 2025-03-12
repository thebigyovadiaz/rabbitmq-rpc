package main

import (
	"context"
	"fmt"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/thebigyovadiaz/rabbitmq-rpc/src/util"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"time"
)

func randomString(l int) string {
	bytes := make([]byte, l)
	for i := 0; i < l; i++ {
		bytes[i] = byte(randInt(65, 90))
	}
	return string(bytes)
}

func randInt(min int, max int) int {
	return min + rand.Intn(max-min)
}

func bodyFrom(args []string) int {
	var s string
	if (len(args) < 2) || os.Args[1] == "" {
		s = "30"
	} else {
		s = strings.Join(args[1:], " ")
	}

	n, err := strconv.Atoi(s)
	util.LogFailOnError(err, "Failed to convert arg to integer")
	return n
}

func fibonacciRPC(n int) (res int, err error) {
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672/")
	util.LogFailOnError(err, "Failed to connect to RabbitMQ")
	util.LogSuccessful("RabbitMQ connect successfully")
	defer conn.Close()

	ch, err := conn.Channel()
	util.LogFailOnError(err, "Failed to open a channel")
	util.LogSuccessful("Channel open successfully")
	defer ch.Close()

	qD, err := ch.QueueDeclare(
		"",
		false,
		false,
		true,
		false,
		nil,
	)
	util.LogFailOnError(err, "Failed to declare a queue")

	messages, err := ch.Consume(
		qD.Name,
		"",
		true,
		false,
		false,
		false,
		nil,
	)
	util.LogFailOnError(err, "Failed to register a consumer")

	corrId := randomString(32)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = ch.PublishWithContext(
		ctx,
		"",
		"rpc_queue",
		false,
		false,
		amqp.Publishing{
			ContentType:   "text/plain",
			CorrelationId: corrId,
			ReplyTo:       qD.Name,
			Body:          []byte(strconv.Itoa(n)),
		},
	)
	util.LogFailOnError(err, "Failed to publish a message")

	for d := range messages {
		if corrId == d.CorrelationId {
			res, err = strconv.Atoi(string(d.Body))
			util.LogFailOnError(err, "Failed to convert body to integer")
			util.LogSuccessful(fmt.Sprintf("Converted body to integer successfully: %d", res))
			break
		}
	}

	return
}

func main() {
	rand.Seed(time.Now().UTC().UnixNano())

	n := bodyFrom(os.Args)

	util.LogSuccessful(fmt.Sprintf("Requesting fib(%d)", n))

	res, err := fibonacciRPC(n)
	util.LogFailOnError(err, "Failed to handle RPC request")
	util.LogSuccessful(fmt.Sprintf(" [.] Got %d", res))
}

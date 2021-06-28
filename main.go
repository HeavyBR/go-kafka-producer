package main

import (
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/joho/godotenv"
	"gopkg.in/confluentinc/confluent-kafka-go.v1/kafka"
)

var topic = flag.String("topic", "meu.topico", "topic that this producer will use")
var message = flag.String("message", "", "message that this producer will send")

func main() {
	var err error

	err = godotenv.Load()
	if err != nil {
		log.Fatal("Error loading .env file")
	}

	flag.Parse()

	p, err := kafka.NewProducer(&kafka.ConfigMap{
		"bootstrap.servers": func(kafkaHost string) string {
			if kafkaHost == "" {
				return "localhost:29092"
			}

			return kafkaHost
		}(os.Getenv("KAFKA_SERVER_HOST")),
	})

	if err != nil {
		log.Fatalf("Cannot create Producer: %v", err)
	}

	// Channel that will wait for messages results
	deliveryChan := make(chan kafka.Event)
	err = p.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{
			Topic:     topic,
			Partition: kafka.PartitionAny,
		},
		Value:   []byte(*message),
		Headers: []kafka.Header{{Key: "myTestHeader", Value: []byte("header values are binary")}},
	}, deliveryChan)

	if err != nil {
		log.Fatalf("something went wrong on producing message %s on topic %s: %v", *message, *topic, err)
	}

	e := <-deliveryChan
	m := e.(*kafka.Message)

	if m.TopicPartition.Error != nil {
		fmt.Printf("Delivery failed: %v\n", m.TopicPartition.Error)
	} else {
		fmt.Printf("Delivered message to topic %s [%d] at offset %v\n", *m.TopicPartition.Topic, m.TopicPartition.Partition, m.TopicPartition.Offset)
	}

	close(deliveryChan)
}

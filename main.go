package main

import (
	"context"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
	pb "github.com/viktorcitaku/kafka-sandbox/proto/v1"
	"google.golang.org/protobuf/proto"
	timestamppb "google.golang.org/protobuf/types/known/timestamppb"
)

func main() {
	topic := "my-topic"
	partition := 0

	conn, err := kafka.DialLeader(context.Background(), "tcp", "localhost:9092", topic, partition)
	if err != nil {
		log.Fatal("failed to dial leader:", err)
	}

	message := messageWithStartingID(1)

	conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
	_, err = conn.WriteMessages(
		kafka.Message{Value: message("one!")},
		kafka.Message{Value: message("two!")},
		kafka.Message{Value: message("three!")},
	)
	if err != nil {
		log.Fatal("failed to write messages:", err)
	}

	if err := conn.Close(); err != nil {
		log.Fatal("failed to close writer:", err)
	}
}

func messageWithStartingID(id int) func(message string) []byte {
	return func(message string) []byte {
		msg := pb.Message{
			Id:        int32(id),
			Text:      message,
			CreatedAt: timestamppb.Now(),
		}

		bytes, err := proto.Marshal(&msg)
		if err != nil {
			log.Fatal("failed to marshal proto message:", err)
		}

		id = id + 1

		return bytes
	}
}

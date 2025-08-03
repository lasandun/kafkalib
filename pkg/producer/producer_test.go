package producer_test

import (
	"testing"

	kafka "github.com/lasandun/kafkalib/pkg/producer"
)

func TestProduce(t *testing.T) {
	producerWrapper := kafka.NewProducerWrapper("localhost:9092")

	if producerWrapper == nil {
		t.Fatalf("Failed to initialize producer")
	}

	err := producerWrapper.Produce("test1", []byte("test message"))
	if err != nil {
		t.Fatalf("Failed to produce message: %v", err)
	}

	producerWrapper.Close()
}

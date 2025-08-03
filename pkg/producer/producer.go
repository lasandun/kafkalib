package producer

import (
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type ProducerWrapper struct {
	producer *kafka.Producer
}

func NewProducerWrapper(broker string) *ProducerWrapper {
	var producerWrapper ProducerWrapper
	var err error = nil

	producerWrapper.producer, err = kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": broker})
	if err != nil {
		fmt.Println("failed to create the producer", err)
		return nil
	}

	return &producerWrapper
}

func (p *ProducerWrapper) Produce(topic string, message []byte) error {
	if p.producer == nil {
		return fmt.Errorf("producer is not created")
	}

	deliveryChan := make(chan kafka.Event, 1)

	err := p.producer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          message,
	}, deliveryChan)

	if err != nil {
		return err
	}

	e := <-deliveryChan
	m := e.(*kafka.Message)

	if m.TopicPartition.Error != nil {
		return m.TopicPartition.Error
	}

	return nil
}

func (p *ProducerWrapper) Close() {
	p.producer.Close()
}

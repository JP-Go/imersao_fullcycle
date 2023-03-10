package main

import (
	// "fmt"
	"fmt"
	"log"

	// "github.com/JP-Go/imersao_fullcycle_simulator/application/route"
	appKafka "github.com/JP-Go/imersao_fullcycle_simulator/application/kafka"
	"github.com/JP-Go/imersao_fullcycle_simulator/infra/kafka"
	ckafka "github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/joho/godotenv"
)

func init(){
  err := godotenv.Load()
  if err != nil {
    log.Fatal("error loading .env file")
  }
}

func main() {
  msgChan := make(chan *ckafka.Message)
  consumer := kafka.NewKafkaConsumer(msgChan)
  go consumer.Consume()

  for msg := range msgChan{
    fmt.Println(string(msg.Value))
    go appKafka.Produce(msg)
  }
}

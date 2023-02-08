package kafka

import (
	"encoding/json"
	"log"
	"os"
	"time"

	"github.com/JP-Go/imersao_fullcycle_simulator/application/route"
	"github.com/JP-Go/imersao_fullcycle_simulator/infra/kafka"
	ckafka "github.com/confluentinc/confluent-kafka-go/kafka"
)

func Produce(msg *ckafka.Message){
  producer := kafka.NewKafkaProducer()
  route := route.NewRoute()
  json.Unmarshal(msg.Value,&route)
  route.LoadPositions()
  positions, err := route.ExportJsonPositions()
  if err != nil {
    log.Println(err.Error())
  }
  for _,p := range positions{
    kafka.Publish(p,os.Getenv("KafkaProduceTopic"),producer)
    time.Sleep(time.Millisecond * 500)
  }
}

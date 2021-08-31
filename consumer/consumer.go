package main

import (
	"fmt"
	"github.com/Shopify/sarama"
	"log"
	"os"
	"os/signal"
	"time"
)

//Kafka consumer
func main() {
	consumer, err := sarama.NewConsumer([]string{"127.0.0.1:9092"}, nil)
	if err != nil {
		panic("消费者连接失败, 报错：" + err.Error())
	}
	defer func() {
		if err := consumer.Close(); err != nil {
			log.Fatalln("关闭消费者失败：" + err.Error())
		}
	}()
	//创建一个分区消费者
	partitionConsumer, err := consumer.ConsumePartition("web_log", 0, sarama.OffsetNewest)
	if err != nil {
		panic("分区消费者创建失败：" + err.Error())
	}
	defer func() {
		if err := partitionConsumer.Close(); err != nil {
			log.Fatalln("关闭分区消费者失败：" + err.Error())
		}
	}()
	// Trap SIGINT to trigger a shutdown.
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)
	consumed := 0
	ConsumerLoop:
	for {
		select {
		case msg := <- partitionConsumer.Messages():
			fmt.Printf("Partition:%d Offset:%d Key:%v Value:%v \n",
				msg.Partition, msg.Offset, string(msg.Key), string(msg.Value))
			consumed++
		case <-signals:
			break ConsumerLoop
		}
	}
	log.Printf("Consumed: %d\n", consumed)

	for {
		time.Sleep(1 * time.Second)
	}
}
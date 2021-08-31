package main

import (
	"fmt"
	"github.com/Shopify/sarama"
	"time"
)

//Kafka producer
func main() {
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll          // 发送完数据需要leader和follow都确认
	config.Producer.Partitioner = sarama.NewRandomPartitioner // 新选出一个partition
	config.Producer.Return.Successes = true                   // 成功交付的消息将在success channel返回
	// 构造一个消息
	msg := &sarama.ProducerMessage{}
	msg.Topic = "web_log"
	msg.Value = sarama.StringEncoder("测试日志")
	// 连接kafka
	client, err := sarama.NewSyncProducer([]string{"127.0.0.1:9092"}, config)
	if err != nil {
		fmt.Println("生产者连接失败, 报错:", err)
		return
	}
	defer client.Close()
	//发送消息
	pid, offset, err := client.SendMessage(msg)
	if err != nil {
		fmt.Println("发送失败, 报错:", err)
		return
	}
	fmt.Printf("pid:%v offset:%v\n", pid, offset)

	for {
		time.Sleep(1 * time.Second)
	}
}
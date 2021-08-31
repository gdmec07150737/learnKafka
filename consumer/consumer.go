package main

import (
	"fmt"
	"github.com/Shopify/sarama"
	"time"
)

//Kafka consumer
func main() {
	consumer, err := sarama.NewConsumer([]string{"127.0.0.1:9092"}, nil)
	if err != nil {
		fmt.Printf("消费者连接失败, 报错:%v\n", err)
		return
	}
	partitionList, err := consumer.Partitions("web_log") // 根据topic取到所有的分区
	if err != nil {
		fmt.Printf("获取分区列表失败，报错：%v\n", err)
		return
	}
	fmt.Println("分区列表：",partitionList)
	for partition := range partitionList { // 遍历所有的分区
		// 针对每个分区创建一个对应的分区消费者
		pc, err := consumer.ConsumePartition("web_log", int32(partition), sarama.OffsetNewest)
		if err != nil {
			fmt.Printf("给 %d 分区分配消费者失败, 报错:%v\n", partition, err)
			return
		}
		defer pc.AsyncClose()
		// 异步从每个分区消费信息
		go func(sarama.PartitionConsumer) {
			fmt.Println("进来了")
			for msg := range pc.Messages() {
				fmt.Printf("Partition:%d Offset:%d Key:%v Value:%v \n",
					msg.Partition, msg.Offset, string(msg.Key), string(msg.Value))
			}
		}(pc)
	}

	for {
		time.Sleep(1 * time.Second)
	}
}
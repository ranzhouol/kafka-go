package main

import (
	"context"
	"fmt"
	"github.com/segmentio/kafka-go"
	"github.com/sirupsen/logrus"
	"log"
	"myfile/MQ/kafka/setting"
	"time"
)

func main() {
	// 读取配置
	if err := setting.InitSetting(); err != nil {
		log.Fatalf("初始化设置错误:%v", err.Error())
	}

	// 通过Reader接收消息
	CustomByReader()

	// 消费者组
	//CustomGroupByReader()
}

// CustomByReader 通过Reader接收消息
func CustomByReader() {
	// 创建Reader
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   []string{setting.Conf.Kafka.Address},
		Topic:     setting.Conf.Kafka.Topic,
		Partition: setting.Conf.Kafka.Partition,
		MaxBytes:  10e6, // 10MB
	})

	r.SetOffset(2) // 设置偏移量Offset, 即从第几个消息开始消费

	// 接收消息
	for {
		m, err := r.ReadMessage(context.Background())
		if err != nil {
			logrus.Errorf("ReadMessage err:%v", err.Error())
			break
		}
		fmt.Printf("message at offset %d: %s = %s\n", m.Offset, string(m.Key), string(m.Value))
	}

	// 程序退出前关闭Reader
	if err := r.Close(); err != nil {
		logrus.Fatal("failed to close reader:", err)
	}
}

// CustomGroupByReader 消费者组
func CustomGroupByReader() {
	// 创建一个reader，指定GroupID，从 topic-A 消费消息
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:        []string{setting.Conf.Kafka.Address},
		GroupID:        "consumer-group-id", // 指定消费者组id
		Topic:          setting.Conf.Kafka.Topic,
		MaxBytes:       10e6, // 10MB
		CommitInterval: time.Second,
	})

	//// 接收消息
	// 自动提交
	//for {
	//	m, err := r.ReadMessage(context.Background())
	//	if err != nil {
	//		break
	//	}
	//	fmt.Printf("message at topic/partition/offset %v/%v/%v: %s = %s\n", m.Topic, m.Partition, m.Offset, string(m.Key), string(m.Value))
	//}

	// 显示提交，设置提交间隔CommitInterval
	ctx := context.Background()
	for {
		// 获取消息
		m, err := r.FetchMessage(ctx)
		if err != nil {
			break
		}
		// 处理消息
		fmt.Printf("message at topic/partition/offset %v/%v/%v: %s = %s\n", m.Topic, m.Partition, m.Offset, string(m.Key), string(m.Value))
		// 显式提交
		if err := r.CommitMessages(ctx, m); err != nil {
			log.Fatal("failed to commit messages:", err)
		}
	}

	// 程序退出前关闭Reader
	if err := r.Close(); err != nil {
		log.Fatal("failed to close reader:", err)
	}
}

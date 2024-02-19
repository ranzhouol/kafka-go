package main

import (
	"context"
	"errors"
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

	// 通过Writer发送消息
	//ProduceByWriter()

	// 自动创建topic
	//CreateTopic()

	// 向多个topic发送消息
	ProduceMultiTopic()
}

// ProduceByWriter 通过Writer发送消息
func ProduceByWriter() {
	// 创建一个writer 向topic发送消息
	w := &kafka.Writer{
		Addr:         kafka.TCP(setting.Conf.Kafka.Address),
		Topic:        setting.Conf.Kafka.Topic,
		Balancer:     &kafka.LeastBytes{}, // 指定分区的balancer模式为最小字节分布
		RequiredAcks: kafka.RequireAll,    // ack模式
		Async:        true,                // 异步
	}

	err := w.WriteMessages(context.Background(),
		kafka.Message{
			Key:   []byte("Key-A"),
			Value: []byte("Hello World!"),
		},
		kafka.Message{
			Key:   []byte("Key-B"),
			Value: []byte("One!"),
		},
		kafka.Message{
			Key:   []byte("Key-C"),
			Value: []byte("Two!"),
		},
	)
	if err != nil {
		log.Fatal("failed to write messages:", err)
	}

	if err := w.Close(); err != nil {
		log.Fatal("failed to close writer:", err)
	}

	logrus.Info("writer 发送成功")
}

// CreateTopic 自动创建topic
func CreateTopic() {
	w := &kafka.Writer{
		Addr:                   kafka.TCP(setting.Conf.Kafka.Address),
		Topic:                  "topic-B",
		AllowAutoTopicCreation: true, // 自动创建topic
	}

	messages := []kafka.Message{
		{
			Key:   []byte("Key-A"),
			Value: []byte("Hello World!"),
		},
		{
			Key:   []byte("Key-B"),
			Value: []byte("One!"),
		},
		{
			Key:   []byte("Key-C"),
			Value: []byte("Two!"),
		},
	}

	var err error
	const retries = 3
	// 重试3次
	for i := 0; i < retries; i++ {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		err = w.WriteMessages(ctx, messages...)
		if errors.Is(err, kafka.LeaderNotAvailable) || errors.Is(err, context.DeadlineExceeded) {
			time.Sleep(time.Millisecond * 250)
			continue
		}

		if err != nil {
			log.Fatalf("unexpected error %v", err)
		}
		break
	}

	// 关闭Writer
	if err := w.Close(); err != nil {
		log.Fatal("failed to close writer:", err)
	}

	logrus.Info("writer create topic 成功")
}

// ProduceMultiTopic 向多个topic发送消息
func ProduceMultiTopic() {
	w := &kafka.Writer{
		Addr: kafka.TCP(setting.Conf.Kafka.Address),
		// 注意: 当此处不设置Topic时,后续的每条消息都需要指定Topic
		Balancer: &kafka.LeastBytes{},
	}

	err := w.WriteMessages(context.Background(),
		// 注意: 每条消息都需要指定一个 Topic, 否则就会报错
		kafka.Message{
			Topic: "topic-A",
			Key:   []byte("Key-A"),
			Value: []byte("Hello World!"),
		},
		kafka.Message{
			Topic: "topic-B",
			Key:   []byte("Key-B"),
			Value: []byte("One!"),
		},
		kafka.Message{
			Topic: "topic-C",
			Key:   []byte("Key-C"),
			Value: []byte("Two!"),
		},
	)
	if err != nil {
		log.Fatal("failed to write messages:", err)
	}

	if err := w.Close(); err != nil {
		log.Fatal("failed to close writer:", err)
	}
}

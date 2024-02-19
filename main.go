package main

import (
	"context"
	"github.com/segmentio/kafka-go"
	"github.com/sirupsen/logrus"
	"log"
	"myfile/MQ/kafka/setting"
	"myfile/MQ/kafka/topic"
)

func main() {
	// 读取配置
	if err := setting.InitSetting(); err != nil {
		log.Fatalf("初始化设置错误:%v", err.Error())
	}

	// 连接kafka
	conn, err := Conn()
	if err != nil {
		log.Fatalf("kafka 连接错误:%v", err.Error())
	}
	defer conn.Close()
	logrus.Info("kafka连接成功")

	//// 发送消息
	//if err = producer.Produce(conn); err != nil {
	//	logrus.Errorf("kafka produce err:%v", err.Error())
	//}
	//logrus.Info("kafka发送成功")
	//
	//// 消费消息
	//customer.Custom(conn)

	//// 创建topic
	//if err = topic.Create(conn); err != nil {
	//	log.Fatalf("kafka create topic err:%v", err.Error())
	//}
	//logrus.Info("kafka topic create 成功")

	//// 列出topic
	if err = topic.ListTopic(conn); err != nil {
		log.Fatalf("kafka list topic err:%v", err.Error())
	}
	logrus.Info("kafka list topic 成功")
}

// Conn 连接kafka
func Conn() (*kafka.Conn, error) {
	// 连接至Kafka集群的Leader节点
	conn, err := kafka.DialLeader(context.Background(), "tcp", setting.Conf.Kafka.Address, setting.Conf.Kafka.Topic, setting.Conf.Kafka.Partition)

	return conn, err
}

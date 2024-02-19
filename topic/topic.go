package topic

import (
	"fmt"
	"github.com/segmentio/kafka-go"
	"net"
	"strconv"
)

// Create 创建topic
func Create(conn *kafka.Conn) error {
	// 获取当前控制节点信息
	controller, err := conn.Controller()
	if err != nil {
		return err
	}

	// 连接至leader节点
	controllerConn, err := kafka.Dial("tcp", net.JoinHostPort(controller.Host, strconv.Itoa(controller.Port)))
	if err != nil {
		return err
	}
	defer controllerConn.Close()

	// 创建topic
	topics := []kafka.TopicConfig{
		{
			Topic:             "test1",
			NumPartitions:     1,
			ReplicationFactor: 1,
		},
		{
			Topic:             "test2",
			NumPartitions:     1,
			ReplicationFactor: 1,
		},
	}

	if err = controllerConn.CreateTopics(topics...); err != nil {
		return err
	}

	return nil
}

// ListTopic 列出topic
func ListTopic(conn *kafka.Conn) error {
	// 获取所有分区
	partitions, err := conn.ReadPartitions()
	if err != nil {
		return err
	}

	m := map[string]struct{}{}
	// 遍历所有分区取topic
	for _, p := range partitions {
		m[p.Topic] = struct{}{}
	}
	for k := range m {
		fmt.Println(k)
	}

	return nil
}

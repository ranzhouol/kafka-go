package producer

import (
	"github.com/segmentio/kafka-go"
	"time"
)

func Produce(conn *kafka.Conn) error {
	// 设置发送消息的超时时间
	conn.SetWriteDeadline(time.Now().Add(10 * time.Second))

	// 发送数据
	// WriteMessages将一批消息写入连接的主题和分区，并返回写入的字节数。写操作是原子操作，它要么完全成功，要么完全失败。
	_, err := conn.WriteMessages(
		kafka.Message{Key: []byte("1"), Value: []byte("one!")},
		kafka.Message{Key: []byte("2"), Value: []byte("two!")},
		kafka.Message{Key: []byte("3"), Value: []byte("three!")},
	)
	return err
}

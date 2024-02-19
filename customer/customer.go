package customer

import (
	"fmt"
	"github.com/segmentio/kafka-go"
	"log"
	"time"
)

func Custom(conn *kafka.Conn) {
	// 设置读取超时时间
	conn.SetReadDeadline(time.Now().Add(10 * time.Second))

	// 读取一批消息，得到的batch是一系列消息的迭代器, 设置读取的最小和最大字节数
	batch := conn.ReadBatch(10e3, 1e6) // fetch 10KB min, 1MB max

	// 遍历读取消息
	b := make([]byte, 10e3) // 每条消息最大10KB
	for {
		n, err := batch.Read(b)
		if err != nil {
			break
		}
		fmt.Println(string(b[:n]))
	}

	// 关闭batch
	if err := batch.Close(); err != nil {
		log.Fatal("failed to close batch:", err)
	}
}

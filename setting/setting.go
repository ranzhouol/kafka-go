package setting

import (
	"fmt"
	"github.com/spf13/viper"
)

type Config struct {
	Kafka Kafka
}

type Kafka struct {
	Address   string `mapstructure:"address"`
	Topic     string `mapstructure:"topic"`
	Partition int    `mapstructure:"partition"`
}

var Conf Config

func InitSetting() error {
	// 设置配置文件路径
	viper.AddConfigPath("./config")
	// 配置文件名称(无扩展名)
	viper.SetConfigName("config")
	// 查找并读取配置文件
	if err := viper.ReadInConfig(); err != nil {
		err = fmt.Errorf("viper.ReadInConfig err:%v", err.Error())
		return err
	}

	// 反序列化
	if err := viper.Unmarshal(&Conf); err != nil {
		err = fmt.Errorf("viper.Unmarshal err:%v", err.Error())
		return err
	}

	return nil
}

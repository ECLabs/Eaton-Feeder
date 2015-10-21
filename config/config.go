package config

import (
    "os"
    "strings"
)

var (
    KafkaServers = GetDefaultKafkaServers()
	KafkaTopic   = GetDefaultKafkaTopic()
    KafkaLoggerTopic = GetDefaultKafkaLoggerTopic()
    OffsetType = "newest"
    Debug = false
    parsed = false
)

func IsDebug()bool{
    if parsed  {
        return Debug
    }
    val := os.Getenv("DEBUG")
    Debug = val == "true"
    parsed = true
    return Debug
}
func GetDefaultKafkaServers()[]string{
    val := os.Getenv("KAFKA_SERVERS")
    if val == "" {
        val = "127.0.0.1:9092"
    }
    return strings.Split(val, ",")
}

func GetDefaultKafkaTopic() string {
	val := os.Getenv("KAFKA_TOPIC")
	if val == "" {
		val = "eaton-feeder"
	}
	return val
}

func GetDefaultKafkaLoggerTopic() string {
	val := os.Getenv("KAFKA_LOGGER_TOPIC")
	if val == "" {
		val = "logs"
	}
	return val
}
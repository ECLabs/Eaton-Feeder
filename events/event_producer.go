package events

import (
	"encoding/xml"
	"fmt"
	eatonconfig "github.com/ECLabs/Eaton-Feeder/config"
	"github.com/ECLabs/Eaton-Feeder/ipresolver"
	"github.com/ECLabs/Eaton-Feeder/mapping"
	"github.com/Shopify/sarama"
	"log"
	"path/filepath"
	"runtime"
	"strings"
	"time"
)

const (
	ErrorLevel    = "error"
	InfoLevel     = "info"
	DebugLevel    = "debug"
	TraceLevel    = "trace"
	UnknownCaller = "LOG"
)

var (
	eventPublisher *EventPublisher
)

type EventPublisher struct {
	producer sarama.AsyncProducer
}

func NewEventPublisher() (*EventPublisher, error) {
	config := sarama.NewConfig()
	config.ClientID = ipresolver.GetLocalAddr()
	config.Producer.RequiredAcks = sarama.WaitForLocal
	config.Producer.Compression = sarama.CompressionNone
	config.Producer.Return.Successes = false
	config.Producer.Return.Errors = false
	config.Producer.Partitioner = sarama.NewHashPartitioner
	asyncProducer, err := sarama.NewAsyncProducer(eatonconfig.KafkaServers, config)

	if err != nil {
		return nil, err
	}
	if config.Producer.Return.Successes {
		go func() {
			for msg := range asyncProducer.Successes() {
				log.Println("Sent Message to logs: ", msg.Key)
			}
		}()
	}
	if config.Producer.Return.Errors {
		go func() {
			for err := range asyncProducer.Errors() {
				log.Println("failed to send message to logs: ", err.Error())
			}
		}()
	}
	return &EventPublisher{
		producer: asyncProducer,
	}, nil
}

func GetCallingFunctionName(path string, line int, ok bool) string {
	caller := UnknownCaller
	if !ok {
		return caller
	}
	dir, file := filepath.Split(path)
	if dir != "" {
		dir = filepath.Dir(path)
		split := strings.Split(dir, string(filepath.Separator))
		if len(split) > 0 {
			dir = split[len(split)-1]
		}
		caller = fmt.Sprintf("%s/%s:%d", dir, file, line)
	} else {
		caller = fmt.Sprintf("%s:%d", file, line)
	}
	return caller
}

func Init() error {
	if eventPublisher != nil {
		return nil
	}
	p, err := NewEventPublisher()
	if err != nil {
		return err
	}
	eventPublisher = p
	return nil
}

func Error(msg string, err error) error {
	if err != nil {
		msg = fmt.Sprintf("%s %s", msg, err.Error())
	}
	return eventPublisher.doPublish(2, msg, ErrorLevel)
}

func Info(msg string) error {
	return eventPublisher.doPublish(2, msg, InfoLevel)
}

func Debug(msg string) error {
	return eventPublisher.doPublish(2, msg, DebugLevel)
}

func Trace(msg string) error {
	return eventPublisher.doPublish(2, msg, TraceLevel)
}

func (e *EventPublisher) PublishError(msg string) error {
	return e.doPublish(2, msg, ErrorLevel)
}

func (e *EventPublisher) PublishInfo(msg string) error {
	return e.doPublish(2, msg, InfoLevel)
}

func (e *EventPublisher) PublishDebug(msg string) error {
	return e.doPublish(2, msg, DebugLevel)
}

func (e *EventPublisher) PublishTrace(msg string) error {
	return e.doPublish(2, msg, TraceLevel)
}

func Close() error {
	if eventPublisher != nil {
		err := eventPublisher.Close()
		if err != nil {
			return err
		}
		eventPublisher = nil
	}
	return nil
}

func (e *EventPublisher) doPublish(skip int, msg, level string) error {
	msg = fmt.Sprintf("%s - %s", strings.ToUpper(level), msg)
	log.Println(msg)
	//0 would be the function calling the Caller.
	//1 would be the function calling doPublish.
	//2 is the function calling the function calling doPublish
	_, path, line, ok := runtime.Caller(skip)
	a := &mapping.ApplicationEvent{
		Level:   level,
		Message: msg,
		Date:    time.Now(),
		Logger:  GetCallingFunctionName(path, line, ok),
		Address: ipresolver.GetLocalAddr(),
	}
	if ok {
		a.Path = path
		a.Line = line
	}
	err := e.PublishEvent(a)
	if err != nil {
		log.Println("ERROR - failed to publish msg to kafka servers: ", err)
		return err
	}
	return nil
}

func (e *EventPublisher) PublishEvent(msg *mapping.ApplicationEvent) error {
	data, err := xml.Marshal(msg)
	if err != nil {
		return err
	}
	e.producer.Input() <- &sarama.ProducerMessage{
		Topic: eatonconfig.KafkaLoggerTopic,
		Value: sarama.ByteEncoder(data),
		Key:   sarama.StringEncoder(msg.Date.String()),
	}
	return nil
}

func (e *EventPublisher) Close() error {
	return e.producer.Close()
}

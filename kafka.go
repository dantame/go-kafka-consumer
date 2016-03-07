package main

import (
	"bytes"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"

	"github.com/Shopify/sarama"
	"github.com/linkedin/goavro"
)

func decodeMessage(msg []byte) string {
	schemaString := `{"name":"WebViewMessage","type":"record","fields":[{"name":"app","type":{"name":"Application","type":"record","fields":[{"name":"id","type":{"name":"Unsigned32ByteValue","type":"fixed","size":32}},{"name":"environment","type":{"name":"Environment","type":"enum","symbols":["development","test","pre_production","production"]}}]}},{"name":"timestamp","type":"long"},{"name":"payload","type":{"name":"WebViewEvent","type":"record","fields":[{"name":"application","type":"Application"},{"name":"type","type":{"name":"WebViewEventType","type":"enum","symbols":["view"]}},{"name":"timestamp","type":"long"},{"name":"context","type":{"name":"WebViewEventContext","type":"record","fields":[{"name":"page","type":{"name":"Page","type":"record","fields":[{"name":"name","type":"string"},{"name":"url","type":{"name":"Url","type":"record","fields":[{"name":"canonical","type":"string"},{"name":"aliases","type":{"type":"array","items":"string"}}]}},{"name":"referrer","type":"string"},{"name":"geoRegion","type":"string"},{"name":"breadcrumb","type":{"type":"array","items":"string"}},{"name":"custom","type":{"type":"map","values":[{"type":"array","items":"string"},"boolean","string","long",{"type":"map","values":[{"type":"array","items":"string"},"boolean","string","long",{"type":"map","values":[{"type":"array","items":"string"},"boolean","string","long",{"type":"map","values":[{"type":"array","items":"string"},"boolean","string","long"]}]}]}]}}]}},{"name":"user","type":{"name":"User","type":"record","fields":[{"name":"trackingId","type":"string"},{"name":"geoRegion","type":"string"},{"name":"custom","type":{"type":"map","values":[{"type":"array","items":"string"},"boolean","string","long",{"type":"map","values":[{"type":"array","items":"string"},"boolean","string","long",{"type":"map","values":[{"type":"array","items":"string"},"boolean","string","long",{"type":"map","values":[{"type":"array","items":"string"},"boolean","string","long"]}]}]}]}}]}},{"name":"device","type":{"name":"Device","type":"record","fields":[{"name":"uuid","type":"Unsigned32ByteValue"},{"name":"windowId","type":"Unsigned32ByteValue"},{"name":"resolution","type":{"name":"Resolution","type":"record","fields":[{"name":"width","type":"long"},{"name":"height","type":"long"}]}},{"name":"ip","type":"string"},{"name":"custom","type":{"type":"map","values":[{"type":"array","items":"string"},"boolean","string","long",{"type":"map","values":[{"type":"array","items":"string"},"boolean","string","long",{"type":"map","values":[{"type":"array","items":"string"},"boolean","string","long",{"type":"map","values":[{"type":"array","items":"string"},"boolean","string","long"]}]}]}]}}]}},{"name":"custom","type":{"type":"array","items":{"name":"Custom","type":"record","fields":[{"name":"name","type":"string"},{"name":"custom","type":{"type":"map","values":[{"type":"array","items":"string"},"boolean","string","long",{"type":"map","values":[{"type":"array","items":"string"},"boolean","string","long",{"type":"map","values":[{"type":"array","items":"string"},"boolean","string","long",{"type":"map","values":[{"type":"array","items":"string"},"boolean","string","long"]}]}]}]}}]}}},{"name":"vendor","type":{"type":"array","items":[{"name":"Adobe","type":"record","fields":[{"name":"type","type":{"name":"AdobeContextType","type":"enum","symbols":["adobe"]}},{"name":"events","type":{"type":"array","items":"string"}}]}]}}]}}]}}]}`
	codec, err := goavro.NewCodec(schemaString)
	if err != nil {
		panic(err)
	}
	bb := bytes.NewBuffer(msg)
	decoded, err := codec.Decode(bb)
	if err != nil {
		panic(err)
	}
	return decoded.(string)
}

func ConsumeFromKafka() {
	conn := ConnectToZk()
	defer conn.Close()

	brokerList := GetBrokersFromZk(conn)
	topicList := make([]string, 1)
	topicList[0] = "web-view-message"
	//topicList := GetTopicsFromZk(conn)

	var initialOffset int64
	switch *offset {
	case "oldest":
		initialOffset = sarama.OffsetOldest
	case "newest":
		initialOffset = sarama.OffsetNewest
	default:
		PrintUsageErrorAndExit("-offset should be `oldest` or `newest`")
	}

	fmt.Println(brokerList)
	consumer, err := sarama.NewConsumer(brokerList, nil)
	must(err)

	var (
		messages = make(chan *sarama.ConsumerMessage, *bufferSize)
		closing  = make(chan struct{})
		wg       sync.WaitGroup
	)

	go func() {
		signals := make(chan os.Signal, 1)
		signal.Notify(signals, os.Kill, os.Interrupt)
		<-signals
		logger.Println("Initiating shutdown of consumer...")
		close(closing)
	}()

	for _, topic := range topicList {
		partitionList, err := getPartitions(consumer, topic)
		must(err)
		for _, partition := range partitionList {
			pc, err := consumer.ConsumePartition(topic, partition, initialOffset)
			must(err)

			go func(pc sarama.PartitionConsumer) {
				<-closing
				pc.AsyncClose()
			}(pc)

			wg.Add(1)
			go func(pc sarama.PartitionConsumer) {
				defer wg.Done()
				for message := range pc.Messages() {
					messages <- message
				}
			}(pc)
		}
	}

	go func() {
		for msg := range messages {
			fmt.Printf("Undecoded:\t%v\n", msg.Value)
			decoded := decodeMessage(msg.Value)
			fmt.Printf("Topic:\t%s\n", msg.Topic)
			fmt.Printf("Partition:\t%d\n", msg.Partition)
			fmt.Printf("Offset:\t%d\n", msg.Offset)
			fmt.Printf("Key:\t%s\n", string(msg.Key))
			fmt.Printf("Value:\t%s\n", decoded)
			fmt.Println()
		}
	}()

	wg.Wait()
	logger.Println("Done consuming all topics")
	close(messages)

	if err := consumer.Close(); err != nil {
		logger.Println("Failed to close consumer: ", err)
	}
}

func getPartitions(c sarama.Consumer, topic string) ([]int32, error) {
	if *partitions == "all" {
		return c.Partitions(topic)
	}

	tmp := strings.Split(*partitions, ",")
	var pList []int32
	for i := range tmp {
		val, err := strconv.ParseInt(tmp[i], 10, 32)
		if err != nil {
			return nil, err
		}
		pList = append(pList, int32(val))
	}

	return pList, nil
}

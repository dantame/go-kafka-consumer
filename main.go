package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/samuel/go-zookeeper/zk"
)

var (
	zookeepers = flag.String("zookeepers", os.Getenv("ZOOKEEPER_URLS"), "The comma seperated list of zookeeper instances")
	topic      = flag.String("topic", "", "REQUIRED: the topic to consume")
	partitions = flag.String("partitions", "all", "The partitions to consume, can be 'all' or comma-separated numbers")
	offset     = flag.String("offset", "newest", "The offset to start with. Can be `oldest`, `newest`")
	bufferSize = flag.Int("buffer-size", 256, "The buffer size of the message channel.")

	logger = log.New(os.Stderr, "", log.LstdFlags)
)

func must(err error) {
	if err != nil {
		panic(err)
	}
}

func main() {
	fmt.Println("Hello")

	checkFlags()

	consumeFromKafka()

}

func checkFlags() {
	flag.Parse()

	if *zookeepers == "" {
		printUsageErrorAndExit("You have to provide -zookeepers as a comma-separated list, or set the ZOOKEEPER_URLS environment variable")
	}

	if *topic == "" {
		printUsageErrorAndExit("-topic is required")
	}
}

func consumeFromKafka() {
	brokerList := getBrokersFromZk()

	var initialOffset int64
	switch *offset {
	case "oldest":
		initialOffset = sarama.OffsetOldest
	case "newest":
		initialOffset = sarama.OffsetNewest
	default:
		printUsageErrorAndExit("-offset should be `oldest` or `newest`")
	}

	consumer, err := sarama.NewConsumer(brokerList, nil)
	must(err)

	partitionList, err := getPartitions(consumer)
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

	for _, partition := range partitionList {
		pc, err := consumer.ConsumePartition(*topic, partition, initialOffset)
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

	go func() {
		for msg := range messages {
			fmt.Printf("Partition:\t%d\n", msg.Partition)
			fmt.Printf("Offset:\t%d\n", msg.Offset)
			fmt.Printf("Key:\t%s\n", string(msg.Key))
			fmt.Printf("Value:\t%s\n", string(msg.Value))
			fmt.Println()
		}
	}()

	wg.Wait()
	logger.Println("Done consuming topic", *topic)
	close(messages)

	if err := consumer.Close(); err != nil {
		logger.Println("Failed to close consumer: ", err)
	}
}

func getPartitions(c sarama.Consumer) ([]int32, error) {
	if *partitions == "all" {
		return c.Partitions(*topic)
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

func connectToZk() *zk.Conn {
	zkServers := strings.Split(*zookeepers, ",")
	conn, _, err := zk.Connect(zkServers, time.Second)
	must(err)
	return conn
}

func getBrokersFromZk() []string {
	conn := connectToZk()
	defer conn.Close()

	brokerIds, _, err := conn.Children("/brokers/ids")
	must(err)

	brokerAddrs := make([]string, len(brokerIds))
	for i, brokerId := range brokerIds {
		brokerAddr, _, err := conn.Get("/brokers/ids/" + brokerId)
		must(err)
		var addr interface{}
		jsonErr := json.Unmarshal(brokerAddr, &addr)
		must(jsonErr)
		addrMap := addr.(map[string]interface{})
		host := addrMap["host"].(string)
		port := strconv.FormatFloat(addrMap["port"].(float64), 'f', -1, 64)
		brokerAddrs[i] = host + ":" + port
	}
	return brokerAddrs
}

func printUsageErrorAndExit(format string, values ...interface{}) {
	fmt.Fprintf(os.Stderr, "ERROR: %s\n", fmt.Sprintf(format, values...))
	fmt.Fprintln(os.Stderr)
	fmt.Fprintln(os.Stderr, "Available command line options:")
	flag.PrintDefaults()
	os.Exit(64)
}

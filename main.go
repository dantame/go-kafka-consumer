package main

import (
	"flag"
	"fmt"
	"log"
	"os"
)

var (
	zookeepers = flag.String("zookeepers", os.Getenv("ZOOKEEPER_URLS"), "The comma seperated list of zookeeper instances")
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

	ConsumeFromKafka()
}

func checkFlags() {
	flag.Parse()

	if *zookeepers == "" {
		PrintUsageErrorAndExit("You have to provide -zookeepers as a comma-separated list, or set the ZOOKEEPER_URLS environment variable")
	}
}

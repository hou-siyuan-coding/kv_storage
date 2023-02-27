package main

import (
	"fmt"
	"kv_storage/config"
	"kv_storage/server/tcp"
	"os"
)

var defaultProperties = &config.Config{
	Bind:    "localhost",
	Port:    8002,
	AofFile: "../backups/cmdLog2.txt",
}

func main() {
	configFilename := os.Getenv("CONFIG")
	fmt.Println(configFilename)
	if configFilename == "" {
		if fileExists("redis.conf") {
			config.SetupConfig("redis.conf")
			fmt.Println("redis.conf")
		} else {
			config.Properties = defaultProperties
			fmt.Println("default.conf")
		}
	} else {
		config.SetupConfig(configFilename)
	}
	if len(config.Properties.Peers) > 1 {
		config.Properties.IsCluster = true
	}
	fmt.Println(config.Properties)
	server := tcp.NewBackend(config.Properties)
	server.Start()
}

func fileExists(filename string) bool {
	info, err := os.Stat(filename)
	return err == nil && !info.IsDir()
}

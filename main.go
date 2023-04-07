package main

import "fmt"

const (
	maxResult     int = 100
	limitPerQuery int = 10
)

type config struct {
}

func main() {
	fmt.Println("Start ...")

	conf, err := initialConfig()
	if err != nil {
		panic(err)
	}

	//for {
	// get data
	//}
	fmt.Println("Done")
}

func initialConfig() (config, error) {
	// initial pg
	// read last status
	return config{}, nil
}

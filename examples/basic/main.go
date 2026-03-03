package main

import (
	"fmt"
	"log"

	"github.com/naipad/petas"
)

func main() {
	db, err := petas.Open("./data", nil)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	// String
	db.SetString("", "name", []byte("Alice"), 0)
	val, ok, _ := db.GetString("", "name")
	if ok {
		fmt.Printf("name: %s\n", val)
	}

	// Hash
	db.HSet("", "user:1", "name", []byte("Bob"))
	db.HSet("", "user:1", "age", []byte("30"))
	name, _ := db.HGet("", "user:1", "name")
	fmt.Printf("user: %s\n", name)

	// ZSet
	db.ZAdd("", "scores", "player1", 100)
	db.ZAdd("", "scores", "player2", 200)
	items, _ := db.ZRange("", "scores", 0, -1)
	for _, item := range items {
		fmt.Printf("%s: %.0f\n", item.Member, item.Score)
	}
}

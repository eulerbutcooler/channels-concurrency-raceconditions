package main

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

var (
	host = "ws://localhost"
)

type TestConfig struct {
	clientCount int
	wg          *sync.WaitGroup
}

func DialServer(t *testing.T, wg *sync.WaitGroup) {
	dialer := websocket.DefaultDialer
	conn, _, err := dialer.Dial(fmt.Sprintf("%s%s", host, WSPort), nil)
	if err != nil {
		t.Errorf("dial failed: %v", err)
		return
	}
	defer func() {
		conn.Close()
		wg.Done()
	}()
	// fmt.Println("connected to the server", conn.LocalAddr().String())
	time.Sleep(1 * time.Second)
}

func TestConnection(t *testing.T) {
	go createWSServer()
	time.Sleep(1 * time.Second)
	tc := TestConfig{
		clientCount: 3,
		wg:          new(sync.WaitGroup),
	}
	tc.wg.Add(tc.clientCount)
	for range tc.clientCount {
		go DialServer(t, tc.wg)

	}
	tc.wg.Wait()
	time.Sleep(2 * time.Second)
	fmt.Println("exiting the test")
}

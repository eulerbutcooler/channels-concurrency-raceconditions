package main

import (
	"encoding/json"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
)

var (
	host      = "ws://localhost"
	roomOneID = uuid.New()
	roomTwoID = uuid.New()
)

type TestConfig struct {
	clientCount    int
	wg             *sync.WaitGroup
	brMsgCount     *atomic.Int64
	targetMsgCount int
}

type RoomTestClient struct {
	conn   *websocket.Conn
	roomID uuid.UUID
	target int
}

func (c *RoomTestClient) readRoomMsgs(wg *sync.WaitGroup, roomOne, roomTwo *atomic.Int64, target int) {
	defer wg.Done()
	var localCount int
	for {
		_, b, err := c.conn.ReadMessage()
		if err != nil {
			return
		}
		var resp RespMsg
		if err := json.Unmarshal(b, &resp); err != nil {
			continue
		}
		switch resp.RoomID {
		case roomOneID:
			roomOne.Add(1)
		case roomTwoID:
			roomTwo.Add(1)
		}
		localCount++
		if localCount == target {
			return
		}
	}
}

func (c *RoomTestClient) send(typ MsgType, data string) error {
	msg := ReqMsg{
		MsgType: typ,
		RoomID:  c.roomID,
		Data:    data,
	}
	return c.conn.WriteJSON(&msg)
}

func DialServer(t *testing.T, wg *sync.WaitGroup, brMsgCount *atomic.Int64, target int) *websocket.Conn {
	exit := make(chan struct{})
	dialer := websocket.DefaultDialer
	conn, _, err := dialer.Dial(fmt.Sprintf("%s%s", host, WSPort), nil)
	if err != nil {
		t.Errorf("dial failed: %v", err)
		wg.Done()
		return nil
	}
	go func() {
		<-exit
		conn.Close()
		wg.Done()
	}()
	// fmt.Println("connected to the server", conn.LocalAddr().String())
	// time.Sleep(1 * time.Second)
	go func() {
		var localCount int
		for {
			_, b, err := conn.ReadMessage()
			if err != nil {
				close(exit)
				return
			}
			if len(b) > 0 {
				localCount++
				brMsgCount.Add(1)
			}
			if localCount == target {
				close(exit)
				return
			}
		}
	}()
	return conn
}

func TestConnection(t *testing.T) {
	s := NewServer()
	go s.createWSServer()
	time.Sleep(1 * time.Second)
	clientCount := 50
	brCount := 3
	tc := TestConfig{
		clientCount:    clientCount,
		wg:             new(sync.WaitGroup),
		brMsgCount:     new(atomic.Int64),
		targetMsgCount: brCount,
	}
	tc.wg.Add(tc.clientCount)
	brConn, _, err := websocket.DefaultDialer.Dial(fmt.Sprintf("%s%s", host, WSPort), nil)
	if err != nil {
		t.Fatal("failed to dial broadcast client")
	}

	defer brConn.Close()

	for range tc.clientCount {
		go DialServer(t, tc.wg, tc.brMsgCount, tc.targetMsgCount)
	}

	for s.GetClientCount() < clientCount+1 {
		time.Sleep(50 * time.Millisecond)
	}

	for range brCount {
		msg := ReqMsg{MsgType: MsgTypeBroadcast, Data: "hello from tests"}
		if err := brConn.WriteJSON(&msg); err != nil {
			t.Errorf("error sending msg %v\n", err)
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	tc.wg.Wait()
	got := tc.brMsgCount.Load()
	want := int64(clientCount * brCount)
	if got != want {
		t.Errorf("expected %d total messages, got %d", want, got)
	} else {
		fmt.Printf("total messages received: %d\n", got)
	}
}

func TestRooms(t *testing.T) {
	s := NewServer()
	go s.createWSServer()
	time.Sleep(1 * time.Second)
	clientCount := 5
	msgCount := 5

	var (
		roomOneSeen atomic.Int64
		roomTwoSeen atomic.Int64
		wg          sync.WaitGroup
	)

	clients := make([]*RoomTestClient, clientCount)
	roomOneClients := 0
	roomTwoClients := 0

	for i := range clientCount {
		conn, _, err := websocket.DefaultDialer.Dial(fmt.Sprintf("%s%s", host, WSPort), nil)
		if err != nil {
			t.Fatalf("dial failed: %v", err)
		}

		roomID := roomOneID
		if i >= clientCount/2 {
			roomID = roomTwoID
		}
		if roomID == roomOneID {
			roomOneClients++
		} else {
			roomTwoClients++
		}
		c := &RoomTestClient{conn: conn, roomID: roomID}
		clients[i] = c

		if err := c.send(MsgTypeJoinRoom, "join"); err != nil {
			t.Errorf("join failed: %v", err)
		}
	}

	for _, c := range clients {
		if c.roomID == roomOneID {
			c.target = msgCount * (roomOneClients - 1)
		} else {
			c.target = msgCount * (roomTwoClients - 1)
		}
	}

	wg.Add(len(clients))
	for _, c := range clients {
		go c.readRoomMsgs(&wg, &roomOneSeen, &roomTwoSeen, c.target)
	}

	for {
		if s.GetRoomClientCount(roomOneID) == roomOneClients &&
			s.GetRoomClientCount(roomTwoID) == roomTwoClients {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	for i := 0; i < roomOneClients; i++ {
		for range msgCount {
			if err := clients[i].send(MsgTypeRoomMsg, "hello"); err != nil {
				t.Errorf("send failed: %v", err)
			}
			time.Sleep(20 * time.Millisecond)
		}
	}

	for i := roomOneClients; i < clientCount; i++ {
		for range msgCount {
			if err := clients[i].send(MsgTypeRoomMsg, "hello"); err != nil {
				t.Errorf("send failed: %v", err)
			}
			time.Sleep(20 * time.Millisecond)
		}
	}
	wg.Wait()

	expectedOne := int64(msgCount * roomOneClients * (roomOneClients - 1))
	expectedTwo := int64(msgCount * roomTwoClients * (roomTwoClients - 1))
	if roomOneSeen.Load() != expectedOne {
		t.Errorf("room one: expected %d, got %d", expectedOne, roomOneSeen.Load())
	}
	if roomTwoSeen.Load() != expectedTwo {
		t.Errorf("room two: expected %d, got %d", expectedTwo, roomTwoSeen.Load())
	}

	for _, c := range clients {
		if err := c.send(MsgTypeLeaveRoom, "leave"); err != nil {
			t.Errorf("leave failed: %v", err)
		}
	}

	for {
		if s.GetRoomClientCount(roomOneID) == 0 &&
			s.GetRoomClientCount(roomTwoID) == 0 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}

	for _, c := range clients {
		c.conn.Close()
	}

	for {
		if s.GetClientCount() == 0 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
}

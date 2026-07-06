package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
)

// TODO:
// [x]HTTP Server
// [x]Upgrade to websocket once client connects
// [x]Add newly connected WS to server
// [x]Add WS client
// []Remove client on disconnect
// []Send broadcast message with no race conditions

type MsgType string

const (
	WSPort                   = ":4444"
	MsgTypeBroadcast MsgType = "broadcast"
)

type ReqMsg struct {
	MsgType MsgType
	Client  *Client
	Data    string
}

type Client struct {
	ID uuid.UUID
	// mu   *sync.RWMutex
	conn *websocket.Conn
}

type Server struct {
	clients map[uuid.UUID]*Client
	// mu      *sync.RWMutex
	joinCh      chan *Client
	leaveCh     chan *Client
	broadcastCh chan *ReqMsg
}

func NewClient(conn *websocket.Conn) *Client {
	ID := uuid.New()
	return &Client{
		ID: ID,
		// mu:   new(sync.RWMutex),
		conn: conn,
	}
}

func NewServer() *Server {
	return &Server{
		clients: map[uuid.UUID]*Client{},
		// mu:      new(sync.RWMutex),
		joinCh:      make(chan *Client, 64),
		leaveCh:     make(chan *Client, 64),
		broadcastCh: make(chan *ReqMsg, 64),
	}
}

func (c *Client) readMsgLoop(leaveCh chan<- *Client, broadcastCh chan<- *ReqMsg) {
	defer func() {
		c.conn.Close()
		leaveCh <- c
	}()
	for {
		_, b, err := c.conn.ReadMessage()
		if err != nil {
			return
		}
		msg := new(ReqMsg)
		err = json.Unmarshal(b, msg)
		if err != nil {
			fmt.Printf("unable to unmarshal the msg %v\n", err)
			continue
		}
		broadcastCh <- msg
	}
}

func (s *Server) AcceptLoop() {
	for {
		select {
		case c := <-s.joinCh:
			// join logic
			s.joinServer(c)
		case c := <-s.leaveCh:
			// leave logic
			s.leaveServer(c)
		case msg := <-s.broadcastCh:
			// broadcast logic
			s.broadcast(msg)
		}
	}
}

func (s *Server) joinServer(c *Client) {
	s.clients[c.ID] = c
	fmt.Printf("client joined the server, cID: %s\n", c.ID)
}

func (s *Server) leaveServer(c *Client) {
	delete(s.clients, c.ID)
	fmt.Printf("client left the server, cID: %s\n", c.ID)
}

func (s *Server) broadcast(m *ReqMsg) {

}

func (s *Server) handleWS(w http.ResponseWriter, r *http.Request) {
	upgrader := websocket.Upgrader{
		ReadBufferSize:  512,
		WriteBufferSize: 512,
		CheckOrigin: func(r *http.Request) bool {
			return true
		},
	}
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		fmt.Printf("Error on HTTP connection upgrade %v\n", err)
		return
	}
	client := NewClient(conn)
	s.joinCh <- client
	go client.readMsgLoop(s.leaveCh, s.broadcastCh)
	// Bad solution.
	// NOTE: Dont communicate by sharing memory. Share memory by communicating.
	// s.mu.Lock()
	// s.clients = append(s.clients, client)
	// fmt.Println(len(s.clients))
	// s.mu.Unlock()

}

func createWSServer() {
	s := NewServer()
	go s.AcceptLoop()
	http.HandleFunc("/", s.handleWS)
	log.Printf("Websocket server listening on %s", WSPort)
	log.Fatal(http.ListenAndServe(WSPort, nil))
}

func main() {
	createWSServer()
}

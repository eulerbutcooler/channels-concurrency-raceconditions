package main

import (
	"fmt"
	"log"
	"net/http"
	"sync"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"
)

// TODO:
// HTTP Server
// Upgrade to websocket once client connects
// Add newly connected WS to server
// Add WS client
// Remove client on disconnect
// Send broadcast message with no race conditions

const (
	WSPort = ":4444"
)

type Client struct {
	ID   uuid.UUID
	mu   *sync.RWMutex
	conn *websocket.Conn
}

type Server struct {
	clients []*Client
	mu      *sync.RWMutex
}

func NewClient(conn *websocket.Conn) *Client {
	ID := uuid.New()
	return &Client{
		ID:   ID,
		mu:   new(sync.RWMutex),
		conn: conn,
	}
}

func NewServer() *Server {
	return &Server{
		clients: []*Client{},
		mu:      new(sync.RWMutex),
	}
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
	// Bad solution.
	// NOTE: Dont communicate by sharing memory. Share memory by communicating.
	s.mu.Lock()
	s.clients = append(s.clients, client)
	fmt.Println(len(s.clients))
	s.mu.Unlock()

}

func createWSServer() {
	s := NewServer()
	http.HandleFunc("/", s.handleWS)
	log.Printf("Websocket server listening on %s", WSPort)
	log.Fatal(http.ListenAndServe(WSPort, nil))
}

func main() {
	createWSServer()
}

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
// [x]Remove client on disconnect
// [x]Send broadcast message with no race conditions

// NOTE: Structs

type MsgType string

const (
	WSPort                   = ":4444"
	MsgTypeBroadcast MsgType = "broadcast"
	MsgTypeJoinRoom  MsgType = "join-room"
	MsgTypeLeaveRoom MsgType = "leave-room"
	MsgTypeRoomMsg   MsgType = "room-message"
)

type Room struct {
	ID      uuid.UUID
	clients map[uuid.UUID]*Client
}

type ReqMsg struct {
	RoomID  uuid.UUID `json:"roomID"`
	MsgType MsgType   `json:"type"`
	Client  *Client   `json:"-"`
	Data    string    `json:"data"`
}

type RespMsg struct {
	MsgType  MsgType   `json:"type"`
	SenderID uuid.UUID `json:"senderID"`
	Data     string    `json:"data"`
	RoomID   uuid.UUID `json:"roomID"`
}

type Client struct {
	ID uuid.UUID
	// mu   *sync.RWMutex
	done  chan struct{}
	msgCh chan *RespMsg
	conn  *websocket.Conn
}

type Server struct {
	clients map[uuid.UUID]*Client
	rooms   map[uuid.UUID]*Room
	// mu          *sync.RWMutex
	joinCh        chan *Client
	leaveCh       chan *Client
	broadcastCh   chan *ReqMsg
	reqCh         chan struct{}
	clientCountCh chan int
	joinRoomCh    chan *ReqMsg
	leaveRoomCh   chan *ReqMsg
	roomMsgCh     chan *ReqMsg
	roomCountCh   chan uuid.UUID
	roomCountResp chan int
}

// NOTE: Constructor pattern
// INFO: Used to initialize the structs so callers wont have to do - make(chan ...)

func NewClient(conn *websocket.Conn) *Client {
	ID := uuid.New()
	return &Client{
		ID:    ID,
		msgCh: make(chan *RespMsg, 64),
		done:  make(chan struct{}), // NOTE: struct{} only because we are only using it to signal closing
		// mu:   new(sync.RWMutex),
		conn: conn,
	}
}

func NewServer() *Server {
	return &Server{
		clients: map[uuid.UUID]*Client{},
		rooms:   map[uuid.UUID]*Room{},
		// mu:          new(sync.RWMutex),
		joinCh:        make(chan *Client, 64),
		leaveCh:       make(chan *Client, 64),
		broadcastCh:   make(chan *ReqMsg, 64),
		reqCh:         make(chan struct{}),
		clientCountCh: make(chan int),
		joinRoomCh:    make(chan *ReqMsg, 64),
		leaveRoomCh:   make(chan *ReqMsg, 64),
		roomMsgCh:     make(chan *ReqMsg, 64),
		roomCountCh:   make(chan uuid.UUID),
		roomCountResp: make(chan int),
	}
}

func NewRespMsg(m *ReqMsg) *RespMsg {
	return &RespMsg{
		MsgType:  m.MsgType,
		Data:     m.Data,
		SenderID: m.Client.ID,
		RoomID:   m.RoomID,
	}
}

func NewRoom(id uuid.UUID) *Room {
	return &Room{
		ID:      id,
		clients: map[uuid.UUID]*Client{},
	}
}

// INFO: Sole owner of conn.WriteJSON() for the client. Runs in its own goroutine (spawned by handleWS)
func (c *Client) writeMsgLoop() {
	defer c.conn.Close()
	for {
		select {
		case <-c.done: // NOTE: Shutdown signal
			return
		case msg := <-c.msgCh: // NOTE: Messages to send
			if err := c.conn.WriteJSON(msg); err != nil {
				fmt.Printf("error sending msg to %s: %v\n", c.ID, err)
				return
			}
		}
	}
}

// INFO: Sole owner of conn.ReadMessage(). It reads bytes and unmarshals it into ReqMsg.
/* NOTE: a better way that follows SOLID principles would be to bundle up the required channels,
 * broadcastCh, leaveRoomCh, joinRoomCh, roomMsgCh into a struct and then pass it to the function
 * instead of the whole server struct
 */
func (c *Client) readMsgLoop(s *Server) {
	defer func() {
		// INFO: here the done is closed first making sure the writer is drained before closing the client
		close(c.done)
		// INFO: leaveCh sends the client left message
		s.leaveCh <- c
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
		msg.Client = c
		switch msg.MsgType {
		case MsgTypeBroadcast:
			s.broadcastCh <- msg
		case MsgTypeJoinRoom:
			s.joinRoomCh <- msg
		case MsgTypeLeaveRoom:
			s.leaveRoomCh <- msg
		case MsgTypeRoomMsg:
			s.roomMsgCh <- msg
		default:
			fmt.Println("unknown msg type")
		}
	}
}

// INFO: Single goroutine that owns s.clients and s.rooms. Every mutation of those maps happens here.
// NOTE: Currently this is non concurrent. An alternative would be to have a worker pool per channel.
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
		case <-s.reqCh:
			s.clientCountCh <- len(s.clients)
		case msg := <-s.joinRoomCh:
			s.joinRoom(msg)
		case msg := <-s.leaveRoomCh:
			s.leaveRoom(msg)
		case msg := <-s.roomMsgCh:
			s.roomMsg(msg)
		case roomID := <-s.roomCountCh:
			room, ok := s.rooms[roomID]
			if !ok {
				s.roomCountResp <- 0
			} else {
				s.roomCountResp <- len(room.clients)
			}
		}
	}
}

func (s *Server) GetClientCount() int {
	s.reqCh <- struct{}{}
	return <-s.clientCountCh
}

// INFO: Adds client to the server's main map.
func (s *Server) joinServer(c *Client) {
	s.clients[c.ID] = c
	fmt.Printf("client joined the server, cID: %s\n", c.ID)
}

/*
INFO: Removes the client from the server and every room it joined.
Also removes any empty rooms
*/
func (s *Server) leaveServer(c *Client) {
	delete(s.clients, c.ID)
	for i, r := range s.rooms {
		delete(r.clients, c.ID)
		if len(r.clients) == 0 {
			delete(s.rooms, i)
		}
	}

}

// INFO: Iterates all clients, skips the sender and drops the response into each receiver's outbox
// INFO: The writeMsgLoop picks up the messages and sends them
func (s *Server) broadcast(m *ReqMsg) {
	resp := NewRespMsg(m)
	for id, c := range s.clients {
		if id == m.Client.ID {
			continue
		}
		c.msgCh <- resp
	}
}

// INFO: First joiner materializes the room.
func (s *Server) joinRoom(m *ReqMsg) {
	room, ok := s.rooms[m.RoomID]
	if !ok {
		room = NewRoom(m.RoomID)
		s.rooms[m.RoomID] = room
	}
	room.clients[m.Client.ID] = m.Client
}

// INFO: If room doesn't exists we just return. Empty rooms are deleted.
func (s *Server) leaveRoom(m *ReqMsg) {
	room, ok := s.rooms[m.RoomID]
	if !ok {
		return
	}
	delete(room.clients, m.Client.ID)
	if len(room.clients) == 0 {
		delete(s.rooms, m.RoomID)
	}
}

// INFO: Checks if the room exists and the sender is in the room then just sends the msg to everyone except the sender
func (s *Server) roomMsg(m *ReqMsg) {
	room, ok := s.rooms[m.RoomID]
	if !ok {
		return
	}
	_, senderInRoom := room.clients[m.Client.ID]
	if !senderInRoom {
		return
	}
	resp := NewRespMsg(m)
	for id, c := range room.clients {
		if id == m.Client.ID {
			continue
		}
		c.msgCh <- resp
	}
}

/*
INFO: These run on the caller's goroutine.
Uses 2 channels to send and receive the number of existing rooms.
*/
func (s *Server) GetRoomClientCount(roomID uuid.UUID) int {
	s.roomCountCh <- roomID
	return <-s.roomCountResp
}

/*
INFO: Only public entry point for connection. Upgrades the http connection to websocket.
Creates a client wrapping the upgraded connection.
Spawns the goroutines - reader and writer
*/
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
	go client.readMsgLoop(s)
	go client.writeMsgLoop()
	// // Bad solution.
	// // NOTE: Dont communicate by sharing memory. Share memory by communicating.
	// s.mu.Lock()
	// s.clients = append(s.clients, client)
	// fmt.Println(len(s.clients))
	// s.mu.Unlock()

}

// INFO: Server bootstrap. Starts the AcceptLoop as well.
func (s *Server) createWSServer() {
	go s.AcceptLoop()
	mux := http.NewServeMux()
	mux.HandleFunc("/", s.handleWS)
	log.Printf("Websocket server listening on %s", WSPort)
	log.Fatal(http.ListenAndServe(WSPort, mux))
}

func main() {
	srv := NewServer()
	srv.createWSServer()
}

package main

import (
	"fmt"
	"log"
	"net/http"
	"sync"

	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

type Client struct {
	conn *websocket.Conn
	send chan []byte
	room *Room
}

type Room struct {
	clients    map[*Client]bool
	broadcast  chan []byte
	register   chan *Client
	unregister chan *Client
	mu         sync.RWMutex
}

var (
	rooms   = make(map[string]*Room)
	roomsMu sync.RWMutex
)

func getRoom(name string) *Room {
	roomsMu.Lock()
	defer roomsMu.Unlock()

	if room, exists := rooms[name]; exists {
		return room
	}

	room := &Room{
		clients:    make(map[*Client]bool),
		broadcast:  make(chan []byte, 256),
		register:   make(chan *Client),
		unregister: make(chan *Client),
	}

	go room.run()
	rooms[name] = room
	log.Printf("Created new room: %s", name)
	return room
}

func (r *Room) run() {
	for {
		select {
		case client := <-r.register:
			r.mu.Lock()
			r.clients[client] = true
			count := len(r.clients)
			r.mu.Unlock()
			log.Printf("Client connected (total: %d)", count)

		case client := <-r.unregister:
			r.mu.Lock()
			if _, ok := r.clients[client]; ok {
				delete(r.clients, client)
				close(client.send)
				count := len(r.clients)
				r.mu.Unlock()
				log.Printf("Client disconnected (total: %d)", count)
			} else {
				r.mu.Unlock()
			}

		case message := <-r.broadcast:
			r.mu.RLock()
			for client := range r.clients {
				select {
				case client.send <- message:
				default:
					// Client's send buffer is full, skip
				}
			}
			r.mu.RUnlock()
		}
	}
}

func (c *Client) readPump() {
	defer func() {
		c.room.unregister <- c
		c.conn.Close()
	}()

	for {
		messageType, message, err := c.conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseAbnormalClosure) {
				log.Printf("WebSocket error: %v", err)
			}
			break
		}

		// Only handle binary messages (Yjs uses binary)
		if messageType == websocket.BinaryMessage {
			// Broadcast the message to all clients in the room
			c.room.broadcast <- message
		}
	}
}

func (c *Client) writePump() {
	defer func() {
		c.conn.Close()
	}()

	for message := range c.send {
		err := c.conn.WriteMessage(websocket.BinaryMessage, message)
		if err != nil {
			log.Printf("Write error: %v", err)
			return
		}
	}
}

func handleWebSocket(w http.ResponseWriter, r *http.Request) {
	// Get room name from URL path
	roomName := r.URL.Path
	if roomName == "" || roomName == "/" {
		roomName = "/default"
	}

	log.Printf("WebSocket connection request for room: %s", roomName)

	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("Upgrade failed: %v", err)
		return
	}

	log.Printf("✓ WebSocket upgraded successfully")

	room := getRoom(roomName)
	client := &Client{
		conn: conn,
		send: make(chan []byte, 256),
		room: room,
	}

	room.register <- client

	go client.writePump()
	go client.readPump()
}

func main() {
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")

		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			return
		}

		handleWebSocket(w, r)
	})

	port := ":8080"
	fmt.Printf("Yjs WebSocket Server")
	fmt.Printf("Listening on: ws://localhost%s", port)

	log.Fatal(http.ListenAndServe(port, nil))
}

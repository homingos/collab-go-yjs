package main

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"

	"go-yjs-server/server"
	natsync "go-yjs-server/sync"

	"github.com/gorilla/websocket"
)

var upgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool {
		return true // Configure properly for production
	},
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
}

var (
	rooms       = make(map[string]*server.Room)
	roomsMu     sync.RWMutex
	clientID    uint64
	clientIDMu  sync.Mutex
	syncService *natsync.SyncService
)

func getOrCreateRoom(name string) *server.Room {
	roomsMu.Lock()
	defer roomsMu.Unlock()

	room, ok := rooms[name]
	if !ok {
		log.Printf("Room %s not found in memory, creating new room", name)
		room = server.NewRoom(name, syncService)
		rooms[name] = room
		log.Printf("Created room: %s (total rooms: %d)", name, len(rooms))
	} else {
		log.Printf("Room %s found in memory (reusing existing room)", name)
	}
	return room
}

func generateClientID() uint64 {
	clientIDMu.Lock()
	defer clientIDMu.Unlock()
	clientID++
	return clientID
}

func handleWebSocket(w http.ResponseWriter, r *http.Request) {
	roomName := r.URL.Path
	if roomName == "" || roomName == "/" {
		roomName = "/default"
	}

	log.Printf("WebSocket connection request for room: %s", roomName)

	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("Upgrade error: %v", err)
		return
	}

	room := getOrCreateRoom(roomName)
	clientID := generateClientID()

	client := server.NewClient(conn, room, clientID)
	room.AddClient(client)

	// Send initial sync
	go client.SendInitialSync()

	// Start pumps
	go client.WritePump()
	client.ReadPump()
}

func main() {
	// Get NATS URL from environment or use default
	natsURL := os.Getenv("NATS_URL")
	if natsURL == "" {
		natsURL = "nats://localhost:4222"
	}

	// Generate server ID (use hostname + PID for uniqueness)
	hostname, _ := os.Hostname()
	serverID := hostname + "-" + os.Getenv("SERVER_ID")
	if serverID == hostname+"-" {
		serverID = hostname
	}

	// Initialize NATS sync service
	var err error
	syncService, err = natsync.NewSyncService(natsURL, serverID)
	if err != nil {
		log.Printf("Warning: Failed to connect to NATS at %s: %v", natsURL, err)
		log.Printf("Server will run in single-instance mode (no cross-server sync)")
		syncService = nil
	} else {
		log.Printf("Connected to NATS at %s (Server ID: %s)", natsURL, serverID)
		defer syncService.Close()
	}

	// Handle graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigChan
		log.Println("Shutting down...")
		if syncService != nil {
			syncService.Close()
		}
		os.Exit(0)
	}()

	// HTTP endpoint to fetch document state
	http.HandleFunc("/doc/", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")

		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			return
		}

		if r.Method != "GET" {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}

		// Extract room name from path (e.g., /doc/flow-document -> flow-document)
		roomName := r.URL.Path[len("/doc/"):]
		if roomName == "" {
			roomName = "/default"
		} else {
			roomName = "/" + roomName
		}

		// Get or create room to ensure it's loaded
		room := getOrCreateRoom(roomName)

		// Get document state
		docState := room.GetDocumentState()

		if len(docState) == 0 {
			// No document state, return 204 No Content
			w.WriteHeader(http.StatusNoContent)
			return
		}

		// Return document state as binary
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("Content-Length", fmt.Sprintf("%d", len(docState)))
		w.WriteHeader(http.StatusOK)
		w.Write(docState)
	})

	// WebSocket endpoint
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

	port := "0.0.0.0:8080"
	log.Printf("═══════════════════════════════════════════════════")
	log.Printf("  Y-protocol WebSocket Server")
	log.Printf("═══════════════════════════════════════════════════")
	if syncService != nil {
		log.Printf("  ✓ Distributed mode (NATS JetStream)")
		log.Printf("  ✓ Server ID: %s", serverID)
	} else {
		log.Printf("  ⚠ Single-instance mode (no NATS)")
	}
	log.Printf("  Listening on: ws://%s", port)
	log.Printf("═══════════════════════════════════════════════════")
	log.Fatal(http.ListenAndServe(port, nil))
}

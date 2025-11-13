package server

import (
	"log"
	"sync"

	"go-yjs-server/awareness"
	"go-yjs-server/protocol"
	natsync "go-yjs-server/sync"
	"go-yjs-server/yrs"
)

// Message type constants
const (
	MessageSync           = 0
	MessageAwareness      = 1
	MessageAuth           = 2
	MessageQueryAwareness = 3
)

// Room manages a collaborative document room
type Room struct {
	name        string
	doc         *yrs.Doc
	awareness   *awareness.Awareness
	clients     map[*Client]bool
	syncService *natsync.SyncService
	mu          sync.RWMutex
}

// NewRoom creates a new room and initializes it from KV store if available
func NewRoom(name string, syncService *natsync.SyncService) *Room {
	room := &Room{
		name:        name,
		doc:         yrs.NewDoc(),
		awareness:   awareness.NewAwareness(0), // Server doesn't have a client ID
		clients:     make(map[*Client]bool),
		syncService: syncService,
	}

	// Load document state from KV store
	if syncService != nil {
		docState, err := syncService.LoadDocumentState(name)
		if err != nil {
			log.Printf("Warning: Failed to load document state for room %s: %v", name, err)
		} else if len(docState) > 0 {
			log.Printf("Loading document state for room %s (%d bytes)", name, len(docState))
			txn := room.doc.WriteTransaction(nil)
			if err := txn.Apply(docState); err != nil {
				log.Printf("Error applying document state for room %s: %v", name, err)
			} else {
				log.Printf("Successfully loaded document state for room %s", name)
			}
			txn.Commit()
		}

		// Subscribe to NATS updates for this room (after loading state)
		syncService.SubscribeToRoom(name, func(msgType natsync.MessageType, data []byte) {
			room.handleSyncMessage(msgType, data)
		})
	}

	return room
}

// handleSyncMessage handles messages received from NATS (other servers)
func (r *Room) handleSyncMessage(msgType natsync.MessageType, data []byte) {
	switch msgType {
	case natsync.MessageTypeUpdate:
		// Apply update from another server
		txn := r.doc.WriteTransaction(nil)
		if err := txn.Apply(data); err != nil {
			log.Printf("Error applying sync update for room %s: %v", r.name, err)
			return
		}
		txn.Commit()

		// Broadcast to local clients only (don't send back to NATS)
		encoder := &messageEncoder{}
		encoder.writeVarUint(MessageSync)
		encoder.writeVarUint(protocol.Update)
		encoder.writeBytes(data)

		r.Broadcast(encoder.bytes(), nil)

	case natsync.MessageTypeAwareness:
		// Apply awareness update from another server
		r.awareness.ApplyAwarenessUpdate(data, nil)

		// Broadcast to local clients only
		encoder := &messageEncoder{}
		encoder.writeVarUint(MessageAwareness)
		encoder.writeBytes(data)

		r.Broadcast(encoder.bytes(), nil)
	}
}

// PublishUpdate publishes an update to NATS (for cross-server sync)
func (r *Room) PublishUpdate(update []byte) {
	if r.syncService != nil {
		if err := r.syncService.PublishUpdate(r.name, update); err != nil {
			log.Printf("Error publishing update to NATS for room %s: %v", r.name, err)
		}
	}
}

// PublishAwareness publishes an awareness update to NATS (for cross-server sync)
func (r *Room) PublishAwareness(awarenessUpdate []byte) {
	if r.syncService != nil {
		if err := r.syncService.PublishAwareness(r.name, awarenessUpdate); err != nil {
			log.Printf("Error publishing awareness to NATS for room %s: %v", r.name, err)
		}
	}
}

// AddClient adds a client to the room
func (r *Room) AddClient(c *Client) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.clients[c] = true
}

// RemoveClient removes a client from the room
func (r *Room) RemoveClient(c *Client) {
	r.mu.Lock()
	shouldSave := false

	if _, ok := r.clients[c]; ok {
		delete(r.clients, c)
		close(c.send)

		// Remove client from awareness
		r.awareness.RemoveStates([]uint64{c.clientID}, nil)

		// Check if this is the last client before releasing lock
		shouldSave = len(r.clients) == 0
	}
	r.mu.Unlock()

	// Broadcast awareness update (outside lock)
	r.broadcastAwarenessUpdate()

	// Save document state when last client disconnects
	if shouldSave {
		log.Printf("Last client disconnected for room %s, saving document state", r.name)
		r.SaveDocumentState()
	}
}

// Broadcast sends a message to all clients except the excluded one
func (r *Room) Broadcast(message []byte, exclude *Client) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	for client := range r.clients {
		if client != exclude {
			select {
			case client.send <- message:
			default:
				// Client buffer full, skip
			}
		}
	}
}

// broadcastAwarenessUpdate broadcasts awareness updates to all clients
func (r *Room) broadcastAwarenessUpdate() {
	states := r.awareness.GetStates()
	clients := make([]uint64, 0, len(states))
	for clientID := range states {
		clients = append(clients, clientID)
	}

	awarenessUpdate := r.awareness.EncodeAwarenessUpdate(clients)

	// Publish to NATS for cross-server sync
	r.PublishAwareness(awarenessUpdate)

	encoder := &messageEncoder{}
	encoder.writeVarUint(MessageAwareness)
	encoder.writeBytes(awarenessUpdate)

	r.Broadcast(encoder.bytes(), nil)
}

// GetDoc returns the document for this room

func (r *Room) GetDoc() *yrs.Doc {
	return r.doc
}

// GetAwareness returns the awareness instance for this room
func (r *Room) GetAwareness() *awareness.Awareness {
	return r.awareness
}

// GetDocumentState returns the full document state as a binary update
func (r *Room) GetDocumentState() []byte {
	txn := r.doc.ReadTransaction()
	// Get full state by passing nil state vector
	state := txn.StateDiff(nil)
	txn.Commit()
	return state
}

// SaveDocumentState saves the document state to KV store
func (r *Room) SaveDocumentState() {
	if r.syncService == nil {
		log.Printf("Cannot save document state for room %s: sync service not available", r.name)
		return
	}

	state := r.GetDocumentState()
	if len(state) == 0 {
		log.Printf("Document state is empty for room %s, skipping save", r.name)
		return
	}

	log.Printf("Saving document state for room %s (%d bytes)", r.name, len(state))
	if err := r.syncService.SaveDocumentState(r.name, state); err != nil {
		log.Printf("Error saving document state for room %s: %v", r.name, err)
	} else {
		log.Printf("Successfully saved document state for room %s", r.name)
	}
}
